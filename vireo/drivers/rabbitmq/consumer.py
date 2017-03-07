import json
import threading

from pika.exceptions import ConnectionClosed, ChannelClosed

from ...helper import log

from .helper import active_connection, fill_in_the_blank, SHARED_TOPIC_EXCHANGE_NAME


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
    """
    def __init__(self, url, route, callback, shared_stream, resumable, distributed, queue_options):
        super().__init__(daemon = True)

        self.url            = url
        self.route          = route
        self.callback       = callback
        self.resumable      = resumable
        self.distributed    = distributed
        self.queue_options  = queue_options
        self._shared_stream = shared_stream
        self._channel       = None
        self._queue_name    = None

    @staticmethod
    def can_handle_route(routing_key):
        """ Check if the consumer can handle the given routing key.

            .. note:: the default implementation will handle all routes.

            :param str routing_key: the routing key
        """
        return True

    def run(self):
        with active_connection(self.url) as channel:
            self._channel = channel

            self._queue_name = self._declare_topic_queue(channel) if self.distributed else self._declare_shared_queue(channel)

            # Declare the callback wrapper for this route.
            def callback_wrapper(channel, method_frame, header_frame, body):
                log('debug', 'Method Frame: {}'.format(method_frame))
                log('debug', 'Header Frame: {}'.format(header_frame))
                log('debug', 'Body: {}'.format(header_frame))

                self.callback(json.loads(body.decode('utf8')))

                channel.basic_ack(delivery_tag = method_frame.delivery_tag)

            log('debug', 'Listening to {}'.format(self._debug_route_name()))

            channel.basic_consume(callback_wrapper, self._queue_name)

            # NOTE there is a bug in start_consuming that prevents stop_consuming from cleanly
            #      stopping message consumption. The following is a hack suggested in StackOverflow.
            # channel.start_consuming()
            try:
                while channel._consumer_infos:
                    channel.connection.process_data_events(time_limit = 1)
            except ConnectionClosed:
                log('warning', 'Unexpected connection loss while listening to {}'.format(self._debug_route_name()))

                self._shared_stream.append(SHARED_SIGNAL_CONNECTION_LOSS)

            log('debug', 'Stopped listening to {}'.format(self._debug_route_name()))

    def stop(self):
        """ Stop consumption """
        log('debug', 'Stopping listening to {}'.format(self._debug_route_name()))
        self._channel.stop_consuming()

    def _debug_route_name(self):
        return '{} ({})'.format(self.route, self._queue_name)

    def _declare_shared_queue(self, channel):
        queue_options = fill_in_the_blank(
            {
                'auto_delete': not self.resumable,
                'queue'      : self.route,
            },
            self.queue_options or {}
        )

        channel.queue_declare(**queue_options)

        log('info', 'Declared a queue "{}"'.format(self.route))

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

        log('info', 'Declared a temporary queue "{}"'.format(temp_queue_name))

        log('info', 'Binding a temporary queue "{}" to route {}'.format(temp_queue_name, self.route))
        channel.queue_bind(temp_queue_name, SHARED_TOPIC_EXCHANGE_NAME, self.route)
        log('info', 'Bound a temporary queue "{}" to route {}'.format(temp_queue_name, self.route))

        return temp_queue_name
