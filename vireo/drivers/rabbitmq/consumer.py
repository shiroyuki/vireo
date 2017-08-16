import json
import threading
import traceback
import time

from pika            import BasicProperties
from pika.exceptions import ConnectionClosed, ChannelClosed, IncompatibleProtocolError

from ...helper import log
from ...model  import Message, RemoteSignal

from .exception import NoConnectionError
from .helper import active_connection, fill_in_the_blank, SHARED_TOPIC_EXCHANGE_NAME, SHARED_SIGNAL_CONNECTION_LOSS

MAX_RETRY_COUNT = 120
PING_MESSAGE      = 'ping'


class Consumer(threading.Thread):
    """ Message consumer

        This is used to handle messages on one particular route/queue.

        :param str url: the URL to the RabbitMQ server
        :param str route: the route to observe
        :param callable callback: the callback function / callable object
        :param list shared_stream: the internal message queue for thread synchronization
        :param bool resumable: the flag to indicate whether the consumption is resumable
        :param bool resumable: the flag to indicate whether the messages are distributed evenly across all consumers on the same route
        :param dict queue_options: additional queue options
        :param bool unlimited_retries: the flag to disable limited retry count.
        :param callable on_connect: a callback function when the message consumption begins.
        :param callable on_disconnect: a callback function when the message consumption is interrupted due to unexpected disconnection.
        :param callable on_error: a callback function when the message consumption is interrupted due to exception raised from the main callback function.

        Here is an example for ``on_connect``.

        .. code-block:: Python

            def on_connect(consumer = None):
                ...

        Here is an example for ``on_disconnect``.

        .. code-block:: Python

            def on_disconnect(consumer = None):
                ...

        Here is an example for ``on_error``.

        .. code-block:: Python

            def on_error(exception, consumer = None):
                ...
    """
    def __init__(self, url, route, callback, shared_stream, resumable, distributed, queue_options,
                 simple_handling, unlimited_retries = False, on_connect = None, on_disconnect = None,
                 on_error = None, controller_id = None):
        super().__init__(daemon = True)

        self.url             = url
        self.route           = route
        self.callback        = callback
        self.resumable       = resumable
        self.distributed     = distributed
        self.queue_options   = queue_options
        self.simple_handling = simple_handling
        self._retry_count    = 0
        self._shared_stream  = shared_stream
        self._channel        = None
        self._queue_name     = None
        self._paused         = False
        self._stopped        = False
        self._controller_id  = controller_id

        self._unlimited_retries = unlimited_retries
        self._on_connect        = on_connect
        self._on_disconnect     = on_disconnect
        self._on_error          = on_error

        self._recovery_queue_name = 'RECOVERY.{}'.format(self.route)

        assert not self._on_disconnect or callable(self._on_disconnect), 'The error handler must be callable.'

    @staticmethod
    def can_handle_route(routing_key):
        """ Check if the consumer can handle the given routing key.

            .. note:: the default implementation will handle all routes.

            :param str routing_key: the routing key
        """
        return True

    @property
    def queue_name(self):
        return self._queue_name

    @property
    def stopped(self):
        return self._stopped

    def run(self):
        log('debug', '{}: Active'.format(self._debug_route_name()))

        while not self._stopped:
            try:
                self._listen()
            except NoConnectionError as e:
                self._retry_count += 1

                if self._on_disconnect:
                    self._async_execute(self._on_disconnect)

                    log('error', '{}: Passed the error information occurred to the error handler'.format(self._debug_route_name()))

                if self._retry_count < MAX_RETRY_COUNT:
                    log('info', '{}: Will re-listen to the queue in 1 second ({} attempt(s) left)'.format(self._debug_route_name(), MAX_RETRY_COUNT - self._retry_count))
                    time.sleep(1)
                    log('warning', 'Reconnecting to listen to {}'.format(self._debug_route_name()))

                    continue

                if self._unlimited_retries:
                    log('info', '{}: Will re-listen to the queue in 5 second (unlimited retries)'.format(self._debug_route_name()))
                    time.sleep(5)
                    log('warning', 'Reconnecting to listen to {}'.format(self._debug_route_name()))

                    continue

                log('warning', '{}: Unexpected connection loss detected ({})'.format(self._debug_route_name(), e))

                self._shared_stream.append(SHARED_SIGNAL_CONNECTION_LOSS)

        log('debug', '{}: Inactive'.format(self._debug_route_name()))

    def resume(self):
        if self.stopped:
            log('debug', '{}: Already stopped (resume)'.format(self._debug_route_name()))

            return

        log('debug', '{}: Resuming on listening...'.format(self._debug_route_name()))

        self._paused = False

    def pause(self):
        if self.stopped:
            log('debug', '{}: Already stopped (pause)'.format(self._debug_route_name()))

            return

        log('debug', '{}: Temporarily stop listening...'.format(self._debug_route_name()))

        self._paused = True

    def stop(self):
        """ Stop consumption """
        log('debug', 'Stopping listening to {}...'.format(self._debug_route_name()))

        if self._channel:
            self._channel.stop_consuming()

    def _async_execute(self, callable_method, *args):
        params = [*args]
        params.append(self)

        async_callback = threading.Thread(target = callable_method, args = params, daemon = True)
        async_callback.start()

    def _listen(self):
        with active_connection(self.url, self._on_connect, self._on_disconnect) as channel:
            self._channel = channel

            self._queue_name = self._declare_topic_queue(channel) if self.distributed else self._declare_shared_queue(channel)

            self._declare_recovery_queue(channel)

            # Declare the callback wrapper for this route.
            def callback_wrapper(channel, method_frame, header_frame, body):
                time_sequence = [time.time()]
                raw_message   = body.decode('utf8')

                log('info', '{}: Processing {}...'.format(self._debug_route_name(), raw_message))

                try:
                    # This is inside the try-catch block to deal with malformed data.
                    decoded_message = json.loads(raw_message)
                    remote_signal   = None
                    remote_target   = None

                    if isinstance(decoded_message, dict) and 'remote_signal' in decoded_message:
                        log('debug', '{}: Received a remote signal'.format(self._debug_route_name()))

                        remote_signal = decoded_message['remote_signal']
                        remote_target = decoded_message.get('controller_id', None) or None

                        if remote_signal != RemoteSignal.PING:
                            if not remote_target:
                                log('debug', '{}: Unable to find the remote target.'.format(
                                    self._debug_route_name(),
                                ))

                                # Acknowledge the message to discard it as it is an invalid remote command.
                                channel.basic_ack(delivery_tag = method_frame.delivery_tag)

                                log('debug', '{}: Discard to an invalid remote command.'.format(
                                    self._debug_route_name(),
                                ))

                                return

                            elif remote_target != self._controller_id:
                                log('debug', '{}: Ignoring the remote signal (TARGET {}).'.format(
                                    self._debug_route_name(),
                                    remote_target
                                ))

                                # Not acknowledge the message to requeue it as this remote command
                                # is not for the current consumer.
                                channel.basic_nack(delivery_tag = method_frame.delivery_tag)

                                log('debug', '{}: Ignored the remote signal (TARGET {}).'.format(
                                    self._debug_route_name(),
                                    remote_target
                                ))

                                return

                    time_sequence.append(time.time())

                    if remote_signal == RemoteSignal.PING:
                        log('debug', '{}: Detected PING signal'.format(self._debug_route_name()))

                        channel.basic_ack(delivery_tag = method_frame.delivery_tag)

                        log('debug', '{}: Ready (post-ping)'.format(self._debug_route_name()))

                        return

                    if remote_signal == RemoteSignal.RESUME:
                        log('debug', '{}: Receive RESUME signal'.format(self._debug_route_name()))

                        self.resume()

                        channel.basic_ack(delivery_tag = method_frame.delivery_tag)

                        log('debug', '{}: Reactivated'.format(self._debug_route_name()))

                        return

                    if remote_signal == RemoteSignal.PAUSE:
                        log('debug', '{}: Receive PAUSE signal'.format(self._debug_route_name()))

                        self.pause()

                        channel.basic_ack(delivery_tag = method_frame.delivery_tag)

                        log('debug', '{}: Standing by...'.format(self._debug_route_name()))

                        return

                    if self._paused:
                        log('info', '{}: On STANDBY'.format(self._debug_route_name()))

                        channel.basic_nack(delivery_tag = method_frame.delivery_tag)

                        log('debug', '{}: Temporarily block itself for a moment'.format(self._debug_route_name()))

                        time.sleep(3)

                        log('debug', '{}: Ready (standby)'.format(self._debug_route_name()))

                        return

                    message = (
                        decoded_message
                        if self.simple_handling
                        else Message(
                            decoded_message,
                            {
                                'header': header_frame,
                                'method': method_frame,
                            }
                        )
                    )

                    self.callback(message)

                    # Acknowledge the delivery after the work is done.
                    channel.basic_ack(delivery_tag = method_frame.delivery_tag)
                except Exception as unexpected_error:
                    error_info = {
                        'type'      : type(unexpected_error).__name__,
                        'message'   : str(unexpected_error),
                        'traceback' : traceback.format_exc(),
                    }
                    log('error', '{}: Exception raised while processing the message: {type}: {message}\n{traceback}'.format(
                        self._debug_route_name(),
                        **error_info,
                    ))

                    # Store the message that cause error in the recovery queue.
                    republishing_options = {
                        'exchange'    : '',
                        'routing_key' : self._recovery_queue_name,
                        'properties'  : BasicProperties(content_type = 'application/json'),
                        'body'        : json.dumps(
                                            {
                                                'controller' : self._controller_id,
                                                'error'      : error_info,
                                                'message'    : raw_message,
                                                'when'       : time.time(),
                                            },
                                            indent = 4
                                        ),
                    }

                    channel.basic_publish(**republishing_options)

                    # Acknowledge the delivery when an error occurs to DEQUEUE the message.
                    channel.basic_ack(delivery_tag = method_frame.delivery_tag)

                    if self._on_error:
                        self._async_execute(self._on_error, unexpected_error)

                    log('warning', '{}: Recovered from the unexpected error'.format(self._debug_route_name()))

                log('debug', '{}: Ready (OK)'.format(self._debug_route_name()))

            log('debug', '{}: Listening...'.format(self._debug_route_name()))

            channel.basic_consume(callback_wrapper, self._queue_name)

            # NOTE there is a bug in start_consuming that prevents stop_consuming from cleanly
            #      stopping message consumption. The following is a hack suggested in StackOverflow.
            # channel.start_consuming()
            try:
                while channel._consumer_infos:
                    channel.connection.process_data_events(time_limit = 1)

                    if self._retry_count:
                        self._retry_count = 0

                        if self._on_connect:
                            self._async_execute(self._on_connect)

                self._stopped = True
            except ConnectionClosed as e:
                raise NoConnectionError('The connection has been absurbly disconnected.')

            log('debug', 'Stopped listening to {}'.format(self._debug_route_name()))

    def _debug_route_name(self):
        result = 'ROUTE {}/{}'.format(self._controller_id, self.route)

        if self._queue_name:
            return '{} (QUEUE {})'.format(result, self._queue_name)

        return result

    def _declare_shared_queue(self, channel):
        queue_options = fill_in_the_blank(
            {
                'auto_delete': not self.resumable,
                'durable'    : self.resumable,
                'queue'      : self.route,
            },
            self.queue_options or {}
        )

        channel.queue_declare(**queue_options)

        log('info', 'CONTROLLER {}: Declared a queue "{}"'.format(self._controller_id, self.route))

        return self.route

    def _declare_recovery_queue(self, channel):
        queue_options = fill_in_the_blank(
            {
                'auto_delete': not self.resumable,
                'durable'    : self.resumable,
                'queue'      : self._recovery_queue_name,
            },
            self.queue_options or {}
        )

        channel.queue_declare(**queue_options)

        log('info', 'CONTROLLER {}: Declared a recovery queue for ROUTE {}'.format(self._controller_id, self.route))

        return self.route

    def _declare_topic_queue(self, channel):
        # Currently not supporting resumability.
        queue_options = fill_in_the_blank(
            {
                'auto_delete': True,
                'queue'      : '',
            },
            self.queue_options or {}
        )

        channel.exchange_declare(
            exchange      = SHARED_TOPIC_EXCHANGE_NAME,
            exchange_type = 'topic',
            passive       = False,
            durable       = True,
            auto_delete   = False,
        )

        response        = channel.queue_declare(**queue_options)
        temp_queue_name = response.method.queue

        log('info', 'CONTROLLER {}: Declared a temporary queue "{}"'.format(self._controller_id, temp_queue_name))

        log('info', 'CONTROLLER {}: Binding a temporary queue "{}" to route {}'.format(self._controller_id, temp_queue_name, self.route))
        channel.queue_bind(temp_queue_name, SHARED_TOPIC_EXCHANGE_NAME, self.route)
        log('info', 'CONTROLLER {}: Bound a temporary queue "{}" to route {}'.format(self._controller_id, temp_queue_name, self.route))

        return temp_queue_name
