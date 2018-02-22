import json
import math
import sys
import threading
import traceback
import time
import uuid

from pika            import BasicProperties
from pika.exceptions import ConnectionClosed, ChannelClosed, IncompatibleProtocolError

from ...helper import log, debug_mode_enabled
from ...model  import Message, RemoteSignal

from .exception import NoConnectionError
from .helper    import active_connection, fill_in_the_blank, SHARED_DIRECT_EXCHANGE_NAME, SHARED_TOPIC_EXCHANGE_NAME, SHARED_SIGNAL_CONNECTION_LOSS

IMMEDIATE_RETRY_LIMIT = 10
MAX_RETRY_COUNT = 20
MAX_RETRY_DELAY = 30
PING_MESSAGE    = 'ping'


class Consumer(threading.Thread):
    """ Message consumer

        This is used to handle messages on one particular route/queue.

        :param str      url:               the URL to the server
        :param str      route:             the route to observe
        :param callable callback:          the callback function / callable object
        :param list     shared_stream:     the internal message queue for thread synchronization
        :param bool     resumable:         the flag to indicate whether the consumption is resumable
        :param bool     resumable:         the flag to indicate whether the messages are distributed evenly across all consumers on the same route
        :param dict     queue_options:     additional queue options
        :param dict     exchange_options:  additional exchange options
        :param bool     unlimited_retries: the flag to disable limited retry count.
        :param callable on_connect:        a callback function when the message consumption begins.
        :param callable on_disconnect:     a callback function when the message consumption is interrupted due to unexpected disconnection.
        :param callable on_error:          a callback function when the message consumption is interrupted due to exception raised from the main callback function.
        :param str controller_id:          the associated controller ID
        :param dict exchange_options:      the additional options for exchange
        :param bool auto_acknowledge:      the flag to determine whether the consumer should auto-acknowledge any delivery (default: ``False``)
        :param bool send_sigterm_on_disconnect: the flag to force the consumer to terminate the process cleanly on disconnection (default: ``True``)
        :param float delay_per_message:    the delay per message (any negative numbers are regarded as zero, zero or any equivalent value is regarded as "no delay")
        :param int max_retries:            the maximum total retries the consumer can have
        :param int immediate_retry_limit:  the maximum immediate retries the consumer can have before it uses the exponential delay

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
                 on_error = None, controller_id = None, exchange_options = None, auto_acknowledge = False,
                 send_sigterm_on_disconnect = True, delay_per_message = 0, max_retries = MAX_RETRY_COUNT,
                 immediate_retry_limit = IMMEDIATE_RETRY_LIMIT, max_retry_delay = MAX_RETRY_DELAY):
        super().__init__(daemon = True)

        queue_options    = queue_options    if queue_options    and isinstance(queue_options,    dict) else {}
        exchange_options = exchange_options if exchange_options and isinstance(exchange_options, dict) else {}

        self.url              = url
        self.route            = route
        self.callback         = callback
        self.resumable        = resumable
        self.distributed      = distributed
        self.queue_options    = queue_options
        self.exchange_options = exchange_options
        self.simple_handling  = simple_handling
        self._retry_count     = 0
        self._shared_stream   = shared_stream
        self._channel         = None
        self._queue_name      = None
        self._paused          = False
        self._stopped         = False
        self._controller_id   = controller_id
        self._consumer_id     = str(uuid.uuid4())
        self._max_retries     = max_retries
        self._max_retry_delay = max_retry_delay
        self._immediate_retry_limit = immediate_retry_limit if immediate_retry_limit < max_retries else max_retries

        self._send_sigterm_on_disconnect = send_sigterm_on_disconnect

        self._delay_per_message = (
            delay_per_message
            if (
                delay_per_message
                and isinstance(delay_per_message, (int, float))
                and delay_per_message > 0
            )
            else 0
        )

        self._auto_acknowledge  = auto_acknowledge
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

                remaining_retries   = self._max_retries - self._retry_count
                can_immediate_retry = self._retry_count <= self._immediate_retry_limit
                wait_time           = 0 if can_immediate_retry else math.pow(2, self._retry_count - self._immediate_retry_limit - 1)

                # Notify the unexpected disconnection
                log('warning', '{}: Unexpected disconnection detected due to {} (retry #{})'.format(self._debug_route_name(), e, self._retry_count))

                # Attempt to retry and skip the rest of error handling routine.
                if remaining_retries >= 0:
                    log(
                        'info',
                        '{}: Will reconnect to the queue in {}s ({} attempt(s) left)'.format(
                            self._debug_route_name(),
                            wait_time,
                            remaining_retries,
                        )
                    )

                    # Give a pause between each retry if the code already retries immediate too often.
                    if wait_time:
                        time.sleep(1)

                    log('warning', '{}: Reconnecting...'.format(self._debug_route_name()))

                    continue
                elif self._on_disconnect:
                    log('warning', '{}: {} the maximum retries (retry #{}/{})'.format(self._debug_route_name(), 'Reached' if self._retry_count == self._max_retries else 'Exceeded', self._retry_count, self._max_retries))

                    self._async_invoke_callback(self._on_disconnect)

                    log('warning', '{}: Passed the error information occurred to the error handler'.format(self._debug_route_name()))

                if self._unlimited_retries:
                    log('info', '{}: Will re-listen to the queue in 5 second (unlimited retries)'.format(self._debug_route_name()))
                    time.sleep(5)
                    log('warning', '{}: Reconnecting...'.format(self._debug_route_name()))

                    continue

                log('warning', '{}: Unexpected connection loss detected ({})'.format(self._debug_route_name(), e))

                self._shared_stream.append(SHARED_SIGNAL_CONNECTION_LOSS)

                if self._send_sigterm_on_disconnect:
                    log('error', '{}: Terminated the process on disconnect.'.format(self._debug_route_name()))

                    sys.exit(1)

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

    def _async_invoke_callback(self, callable_method, *args, **kwargs):
        params = [*args]
        params.append(self)

        kw_params = dict(
            controller_id = self._controller_id,
            route         = self.route,
            queue_name    = self._queue_name,
            **kwargs
        )

        async_callback = threading.Thread(
            target = callable_method,
            args   = params,
            kwargs = kw_params,
            daemon = True,
        )

        async_callback.start()

    def _listen(self):
        with active_connection(self.url, self._on_connect, self._on_disconnect, self._on_error) as channel:
            self._channel = channel

            self._queue_name = (
                self._declare_topic_queue(channel)
                if self.distributed
                else self._declare_shared_queue(channel)
            )

            self._declare_recovery_queue(channel)

            # Declare the callback wrapper for this route.
            def callback_wrapper(channel, method_frame, header_frame, body):
                time_sequence = [time.time()]
                raw_message   = body.decode('utf8')

                message_id = str(uuid.uuid4())

                log('info', '{}: MESSAGE {}: Processing {}...'.format(self._debug_route_name(), message_id, raw_message))

                # Process the message
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
                                if not self._auto_acknowledge:
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

                        if not self._auto_acknowledge:
                            channel.basic_ack(delivery_tag = method_frame.delivery_tag)

                        log('debug', '{}: Ready (post-ping)'.format(self._debug_route_name()))

                        return

                    if remote_signal == RemoteSignal.RESUME:
                        log('debug', '{}: Receive RESUME signal'.format(self._debug_route_name()))

                        self.resume()

                        if not self._auto_acknowledge:
                            channel.basic_ack(delivery_tag = method_frame.delivery_tag)

                        log('debug', '{}: Reactivated'.format(self._debug_route_name()))

                        return

                    if remote_signal == RemoteSignal.PAUSE:
                        log('debug', '{}: Receive PAUSE signal'.format(self._debug_route_name()))

                        self.pause()

                        if not self._auto_acknowledge:
                            channel.basic_ack(delivery_tag = method_frame.delivery_tag)

                        log('debug', '{}: Standing by...'.format(self._debug_route_name()))

                        return

                    if self._paused:
                        log('info', '{}: On STANDBY'.format(self._debug_route_name()))

                        if not self._auto_acknowledge:
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
                except Exception as unexpected_error:
                    self._handle_error_during_consumption(
                        message_id,
                        raw_message,
                        unexpected_error,
                        traceback.format_exc(),
                        'Error detected while processing the message',
                    )

                    log('error', '{}: MESSAGE {}: Error detected while processing the message'.format(self._debug_route_name(), message_id))

                    # Acknowledge the delivery when an error occurs to DEQUEUE the message.
                    if not self._auto_acknowledge:
                        channel.basic_ack(delivery_tag = method_frame.delivery_tag)

                    log('warning', '{}: MESSAGE {}: Recovered from the unexpected error'.format(self._debug_route_name(), message_id))

                # Acknowledge the delivery after the work is done.
                try:
                    if not self._auto_acknowledge:
                        channel.basic_ack(delivery_tag = method_frame.delivery_tag)
                except Exception as unexpected_error:
                    self._handle_error_during_consumption(
                        message_id,
                        raw_message,
                        unexpected_error,
                        traceback.format_exc(),
                        'Error detected while acknowledging the delivery'
                    )

                    log('error', '{}: MESSAGE {}: Error detected while acknowledging the message delivery ({})'.format(self._debug_route_name(), message_id, method_frame.delivery_tag))

                if self._stopped:
                    log('warning', '{}: Consumer terminated'.format(self._debug_route_name()))

                    return

                if self._delay_per_message:
                    log('debug', '{}: Pausing for {:.3f}s (PAUSED)'.format(self._debug_route_name(), self._delay_per_message))

                    time.sleep(self._delay_per_message)

                    log('debug', '{}: Back to action (RESUMED)'.format(self._debug_route_name()))

                log('debug', '{}: Ready (OK)'.format(self._debug_route_name()))

            log('debug', '{}: Listening... (ACK: {})'.format(self._debug_route_name(), 'AUTO' if self._auto_acknowledge else 'MANUAL'))

            channel.basic_consume(
                callback = callback_wrapper,
                queue    = self._queue_name,
                no_ack   = self._auto_acknowledge, # No delivery acknowledgement needed
            )

            try:
                while True:
                    channel.wait()

                    if self._retry_count:
                        self._retry_count = 0

                        if self._on_connect:
                            self._async_invoke_callback(self._on_connect)
                self._stopped = True
            except Exception as e: # ConnectionClosed
                raise NoConnectionError('The connection has been absurbly disconnected.')

            log('debug', 'Stopped listening to {}'.format(self._debug_route_name()))

    def _handle_error_during_consumption(self, message_id, raw_message, error, execution_trace, summary):
        with active_connection(self.url, self._on_connect, self._on_disconnect, self._on_error) as channel:
            error_info = {
                'type'      : type(error).__name__,
                'message'   : str(error),
                'traceback' : execution_trace,
            }

            log('error', '{}: Unexpected error detected: {type}: {message}\n{traceback}'.format(
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
                                    indent = 4,
                                    sort_keys = True,
                                ),
            }

            channel.basic_publish(**republishing_options)

            if self._on_error:
                self._async_invoke_callback(self._on_error, error, summary = summary)

            if isinstance(error, (ConnectionClosed, ChannelClosed)):
                log('error', '{}: Connection Error: {type}: {message}\n{traceback}'.format(
                    self._debug_route_name(),
                    **error_info,
                ))

                raise NoConnectionError()

    def _debug_route_name(self):
        segments = [
            'ROUTE {}'.format(self.route),
            'CONTROLLER {}'.format(self._controller_id),
        ]

        # if debug_mode_enabled:
        #     segments.append('CONSUMER {}'.format(self._consumer_id))

        if self.route != self._queue_name:
            segments.append('QUEUE {}'.format(self._queue_name))

        return '/'.join(segments)

    def _declare_shared_queue(self, channel):
        queue_name = self._declare_queue(
            channel,
            {
                'auto_delete' : not self.resumable,
                'durable'     : self.resumable,
                'queue'       : self.route,
            }
        )

        log('info', '[_declare_shared_queue] CONTROLLER {}: Declared a shared queue "{}"'.format(self._controller_id, queue_name))

        exchange_options = dict(
            exchange      = self.exchange_options.get('name', SHARED_DIRECT_EXCHANGE_NAME),
            exchange_type = self.exchange_options.get('type', 'direct'),
            passive       = self.exchange_options.get('passive', False),
            durable       = self.exchange_options.get('durable', True),
            auto_delete   = self.exchange_options.get('auto_delete', False),
        )

        self._bind_queue(channel, queue_name, exchange_options)

        return self.route

    def _declare_recovery_queue(self, channel):
        queue_name = self._declare_queue(
            channel,
            {
                'auto_delete': not self.resumable,
                'durable'    : self.resumable,
                'queue'      : self._recovery_queue_name,
            }
        )

        log('info', '[_declare_recovery_queue] CONTROLLER {}: Declared a recovery queue for ROUTE {}'.format(self._controller_id, queue_name))

        return self.route

    def _declare_topic_queue(self, channel):
        # Currently not supporting resumability.
        temp_queue_name = self._declare_queue(
            channel,
            {
                'auto_delete': True,
                'queue'      : '',
            }
        )

        log('info', '[_declare_topic_queue] CONTROLLER {}: Declared a distributed queue "{}"'.format(self._controller_id, temp_queue_name))

        exchange_options = dict(
            exchange      = self.exchange_options.get('name', SHARED_TOPIC_EXCHANGE_NAME),
            exchange_type = self.exchange_options.get('type', 'topic'),
            passive       = self.exchange_options.get('passive', False),
            durable       = self.exchange_options.get('durable', True),
            auto_delete   = self.exchange_options.get('auto_delete', False),
        )

        self._bind_queue(channel, temp_queue_name, exchange_options)

        return temp_queue_name

    def _declare_queue(self, channel, default_queue_options):
        queue_options = fill_in_the_blank(
            default_queue_options,
            self.queue_options or {}
        )

        response   = channel.queue_declare(**queue_options)
        queue_name = response.method.queue

        return queue_name

    def _bind_queue(self, channel, queue_name, exchange_options):
        assert 'exchange' in exchange_options

        exchange_name  = exchange_options['exchange']
        debugging_info = (self._controller_id, queue_name, self.route, exchange_name)

        channel.exchange_declare(**exchange_options)

        log('info', '[_bind_queue] CONTROLLER {}: Binding a queue "{}" to route {} on exchange {}'.format(*debugging_info))

        channel.queue_bind(queue_name, exchange_name, self.route)

        log('info', '[_bind_queue] CONTROLLER {}: Bound a queue "{}" to route {} on exchange {}'.format(*debugging_info))
