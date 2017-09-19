import json
import threading
import time
import uuid

from pika            import BasicProperties
from pika.exceptions import ConnectionClosed, ChannelClosed

from ...helper import fill_in_the_blank, log

from .consumer  import Consumer
from .exception import NoConnectionError, SubscriptionNotAllowedError
from .helper    import active_connection, SHARED_DIRECT_EXCHANGE_NAME, SHARED_TOPIC_EXCHANGE_NAME, SHARED_SIGNAL_CONNECTION_LOSS


class Driver(object):
    """ Driver for RabbitMQ

        :param          url:               the URL to the server (``str`` for a single connection or ``list`` for rotation)
        :param list     consumer_classes:  the list of :class:`.consumer.Consumer`-based classes
        :param bool     unlimited_retries: the flag to disable limited retry count.
        :param callable on_connect:        a callback function when the message consumption begins.
        :param callable on_disconnect:     a callback function when the message consumption is interrupted due to unexpected disconnection.
        :param callable on_error:          a callback function when the message consumption is interrupted due to exception raised from the main callback function.

        :param dict default_publishing_options:                  the default options for publishing (normal)
        :param dict default_broadcasting_options:                the default options for publishing (broadcast)
        :param dict default_consuming_shared_queue_options:      the default options for consuming share queue
        :param dict default_consuming_distributed_queue_options: the default options for consuming distributed queue

        ``default_publishing_options`` and ``default_broadcasting_options`` only take ``exchange``
        to allow overriding the default exchange.

        ``default_consuming_shared_queue_options`` and ``default_consuming_distributed_queue_options``
        will have the data structure like this::

            {
                'exchange': {
                    'name': str, # It is "exchange" in pika's exchange_declare.
                    'type': str, # It is "exchange_type" in pika's exchange_declare.
                }
            }

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
    def __init__(self, url, consumer_classes = None, unlimited_retries = False, on_connect = None,
                 on_disconnect = None, on_error = None, default_publishing_options : dict = None,
                 default_broadcasting_options : dict = None, default_consuming_shared_queue_options : dict = None,
                 default_consuming_distributed_queue_options : dict = None):
        for consumer_class in consumer_classes or []:
            assert isinstance(consumer_class, Consumer), 'This ({}) needs to be a subclass of vireo.drivers.rabbitmq.Consumer.'.format(consumer_class)

        self._url              = url
        self._consumer_classes = consumer_classes or []
        self._async_listener   = None
        self._shared_stream    = []
        self._consumers        = []
        self._has_term_signal  = False
        self._active_routes    = []

        self._default_publishing_options                  = default_publishing_options                  or {}
        self._default_broadcasting_options                = default_broadcasting_options                or {}
        self._default_consuming_shared_queue_options      = default_consuming_shared_queue_options      or {}
        self._default_consuming_distributed_queue_options = default_consuming_distributed_queue_options or {}

        self._unlimited_retries = unlimited_retries
        self._on_connect        = on_connect
        self._on_disconnect     = on_disconnect
        self._on_error          = on_error

        self._requested_url_counter = 0 # used for rotation
        self._total_url_count       = 1 if isinstance(self._url, str) else len(self._url)

    @property
    def url(self):
        connection_url = self._url

        if isinstance(self._url, (tuple, list)):
            self._requested_url_counter += 1

            connection_index = self._requested_url_counter % self._total_url_count
            connection_url   = self._url[connection_index]

        return connection_url

    def set_on_connect(self, on_connect):
        self._on_connect = on_connect

    def set_on_disconnect(self, on_disconnect):
        self._on_disconnect = on_disconnect

    def set_on_error(self, on_error):
        self._on_error = on_error

    def setup_async_cleanup(self):
        """ Prepare to cleanly join all consumers asynchronously. """
        if self._async_listener and self._async_listener.is_alive():
            raise SubscriptionNotAllowedError('Unable to consume messages as this driver is currently active.')

        self._async_listener = threading.Thread(target = self.join)
        self._async_listener.start()

    def stop_consuming(self):
        """ Send the signal to stop consumption. """
        self._has_term_signal = True

    def join(self):
        """ Synchronously join all consumers."""
        try:
            while True:
                if self._has_term_signal:
                    log('warning', 'Stopping all route listeners')

                    break

                if SHARED_SIGNAL_CONNECTION_LOSS in self._shared_stream:
                    log('error', 'Unexpected connection loss detected')
                    log('warning', 'Terminating all route listeners')

                    break

                time.sleep(1)
        except KeyboardInterrupt:
            log('warning', 'SIGTERM received')
            log('debug', 'Terminating all route listeners')

        connection_losed = SHARED_SIGNAL_CONNECTION_LOSS in self._shared_stream

        for consumer in self._consumers:
            if not connection_losed:
                if not consumer.is_alive():
                    log('info', 'Route {}: Already stopped listening (not alive).'.format(consumer.route))

                    continue

                log('warning', 'Route {}: Sending the signal to stop listening.'.format(consumer.route))
                consumer.stop()

            try:
                log('debug', 'Route {}: Terminating the listener.'.format(consumer.route))
                consumer._stop()
            except AssertionError: # this is raised if the thread lock is still locked.
                log('warning', 'Route {}: Probably already stopped'.format(consumer.route))

            if not consumer.is_alive():
                log('info', 'Route {}: Termination confirmed (killed)'.format(consumer.route))

                continue

            log('debug', 'Route {}: Waiting the listener to join back to the parent thread.'.format(consumer.route))
            consumer.join()
            log('info', 'Route {}: Termination confirmed (joined).'.format(consumer.route))

        if connection_losed:
            raise NoConnectionError('Unexpectedly losed the connection during message consumption')

    def publish(self, route, message, options = None):
        """ Synchronously publish a message

            :param str route:   the route
            :param str message: the message
            :param dict options: additional options for basic_publish
        """
        default_parameters = self._generate_default_publish_options(
            self._default_publishing_options,
            SHARED_DIRECT_EXCHANGE_NAME,
            route,
            message,
        )

        options = fill_in_the_blank(options or {}, default_parameters)

        with active_connection(self.url, self._on_connect, self._on_disconnect) as channel:
            try:
                log('debug', 'Publishing: route={} message={} options={}'.format(route, message, options))
                channel.basic_publish(**options)
                log('debug', 'Published: route={} message={} options={}'.format(route, message, options))
            except ConnectionClosed:
                raise NoConnectionError('Unexpectedly losed the connection while publishing a message')

    def declare_queue_with_delegation(self, origin_queue_name, ttl, fallback_queue_name = None,
                                      common_queue_options = None, exchange_options = None):
        actual_fallback_queue_name = fallback_queue_name or '{}.delegated'.format(origin_queue_name)
        exchange_name              = 'fallback/{}/{}'.format(origin_queue_name, actual_fallback_queue_name)

        exchange_options     = fill_in_the_blank(exchange_options or {}, {'internal': True})
        common_queue_options = common_queue_options or {}

        default_fallback_queue_options = {'auto_delete': False}
        default_origin_queue_options   = {
            'auto_delete' : False,
            'arguments'   : {
                'x-dead-letter-exchange'    : exchange_name,
                'x-dead-letter-routing-key' : actual_fallback_queue_name,
                'x-message-ttl'             : ttl,
            }
        }

        fallback_queue_options = fill_in_the_blank(dict(common_queue_options), default_fallback_queue_options)
        origin_queue_options   = fill_in_the_blank(dict(common_queue_options), default_origin_queue_options)

        fill_in_the_blank(exchange_options, {'exchange': exchange_name, 'exchange_type': 'direct'})

        with active_connection(self.url, self._on_connect, self._on_disconnect) as channel:
            try:
                channel.exchange_declare(**exchange_options)

                self.declare_queue(actual_fallback_queue_name, fallback_queue_options)

                channel.queue_bind(
                    queue    = actual_fallback_queue_name,
                    exchange = exchange_name,
                )

                self.declare_queue(origin_queue_name, origin_queue_options)
            except ConnectionClosed:
                if self._on_disconnect:
                    async_callback = threading.Thread(target = self._on_disconnect, daemon = True)
                    async_callback.start()

                raise NoConnectionError('Unexpectedly losed the connection while orchestrating queues and exchange for delegation')

    def broadcast(self, route, message, options = None):
        """ Broadcast a message to a particular route.

            :param str route:    the route
            :param str message:  the message
            :param dict options: additional options for basic_publish
        """
        default_parameters = self._generate_default_publish_options(
            self._default_broadcasting_options,
            SHARED_TOPIC_EXCHANGE_NAME,
            route,
            message,
        )

        options = fill_in_the_blank(options or {}, default_parameters)

        if 'exchange' not in options or not options['exchange']:
            options['exchange'] = SHARED_TOPIC_EXCHANGE_NAME

        exchange_name = options['exchange']

        with active_connection(self.url, self._on_connect, self._on_disconnect) as channel:
            try:
                log('debug', 'Declaring a shared topic exchange')

                channel.exchange_declare(
                    exchange      = exchange_name,
                    exchange_type = 'topic',
                    passive       = False,
                    durable       = True,
                    auto_delete   = False,
                )

                log('debug', 'Declared a shared topic exchange')

                log('debug', 'Broadcasting: route={} message={} options={}'.format(route, message, options))
                channel.basic_publish(**options)
                log('debug', 'Broadcasted: route={} message={} options={}'.format(route, message, options))

            except ConnectionClosed:
                if self._on_disconnect:
                    async_callback = threading.Thread(target = self._on_disconnect, daemon = True)
                    async_callback.start()

                raise NoConnectionError('Unexpectedly losed the connection while broadcasting an event')

    def observe(self, route, callback, resumable, distributed, options = None,
                simple_handling = True, controller_id = None):
        consumer_class = Consumer

        for overriding_consumer_class in self._consumer_classes:
            if overriding_consumer_class.can_handle_route(route):
                consumer_class = overriding_consumer_class

                break

        if not controller_id:
            controller_id = str(uuid.uuid4())

            log('info', 'Observer on {} will have the self-assigned controller ID {}'.format(route, controller_id))

        default_options = self._default_consuming_distributed_queue_options if distributed else self._default_consuming_shared_queue_options
        given_options   = options or {}

        queue_options    = fill_in_the_blank(given_options.get('queue',    {}), default_options.get('queue',    {}))
        exchange_options = fill_in_the_blank(given_options.get('exchange', {}), default_options.get('exchange', {}))

        parameters = dict(
            url               = self.url,
            route             = route,
            callback          = callback,
            shared_stream     = self._shared_stream,
            resumable         = resumable,
            distributed       = distributed,
            queue_options     = queue_options,
            simple_handling   = simple_handling,
            unlimited_retries = self._unlimited_retries,
            on_connect        = self._on_connect,
            on_disconnect     = self._on_disconnect,
            on_error          = self._on_error,
            controller_id     = controller_id,
            exchange_options  = exchange_options,
        )

        consumer = consumer_class(**parameters)

        self._consumers.append(consumer)

        consumer.start()

        return consumer

    def _generate_default_publish_options(self, default_publishing_options, default_exchange_name,
                                          route, message):
        return {
            'exchange'    : default_publishing_options.get('exchange', default_exchange_name),
            'routing_key' : route,
            'body'        : json.dumps(message),
            'properties'  : BasicProperties(content_type = 'application/json'),
        }
