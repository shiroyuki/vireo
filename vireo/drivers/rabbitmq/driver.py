import json
import threading
import time

from pika            import BasicProperties
from pika.exceptions import ConnectionClosed, ChannelClosed

from ...helper import fill_in_the_blank, log

from .consumer  import Consumer
from .exception import NoConnectionError, SubscriptionNotAllowedError
from .helper    import active_connection, SHARED_TOPIC_EXCHANGE_NAME, SHARED_SIGNAL_CONNECTION_LOSS


class Driver(object):
    """ Driver for RabbitMQ

        :param str url: the URL to the server
        :param list consumer_classes: the list of Consumer-based classes
    """
    def __init__(self, url, consumer_classes = None):
        for consumer_class in consumer_classes:
            assert isinstance(consumer_class, Consumer), 'This ({}) needs to be a subclass of vireo.drivers.rabbitmq.Consumer.'.format(consumer_class)

        self._url              = url
        self._consumer_classes = consumer_classes
        self._async_listener   = None
        self._shared_stream    = []
        self._consumers        = []
        self._has_term_signal  = False
        self._active_routes    = []

    def setup_async_cleanup(self):
        """ Synchronously join all consumers."""
        if self._async_listener and self._async_listener.is_alive():
            raise SubscriptionNotAllowedError('Unable to consume messages as this driver is currently active.')

        self._async_listener = threading.Thread(target = self.join)
        self._async_listener.start()

    def stop_consuming(self):
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
                log('warning', 'Route {}: Failed to stop cleanly'.format(consumer.route))

            if not consumer.is_alive():
                log('info', 'Route {}: Termination confirmed (killed)'.format(route_listener.route))

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
        """
        default_parameters = {
            'exchange'    : '',
            'routing_key' : route or '',
            'body'        : json.dumps(message),
            'properties'  : BasicProperties(content_type = 'application/json'),
        }

        options = fill_in_the_blank(options or {}, default_parameters)

        with active_connection(self._url) as channel:
            try:
                log('debug', 'Declaring: route={}'.format(route))
                channel.queue_declare(queue = route, passive = True)
                log('debug', 'Declared: route={}'.format(route))

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

        with active_connection(self._url) as channel:
            try:
                channel.exchange_declare(**exchange_options)

                self.declare_queue(actual_fallback_queue_name, fallback_queue_options)

                channel.queue_bind(
                    queue    = actual_fallback_queue_name,
                    exchange = exchange_name,
                )

                self.declare_queue(origin_queue_name, origin_queue_options)
            except ConnectionClosed:
                raise NoConnectionError('Unexpectedly losed the connection while orchestrating queues and exchange for delegation')

    def broadcast(self, route, message, options = None):
        default_parameters = {
            'exchange'    : '',
            'routing_key' : route or '',
            'body'        : json.dumps(message),
            'properties'  : BasicProperties(content_type = 'application/json'),
        }

        options = fill_in_the_blank(options or {}, default_parameters)

        if 'exchange' not in options or not options['exchange']:
            options['exchange'] = SHARED_TOPIC_EXCHANGE_NAME

        exchange_name = options['exchange']

        with active_connection(self._url) as channel:
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
                raise NoConnectionError('Unexpectedly losed the connection while broadcasting an event')

    def observe(self, route, callback, resumable, distributed, options = None):
        consumer_class = Consumer

        for overriding_consumer_class in self._consumer_classes:
            if overriding_consumer_class.can_handle_route(route):
                consumer_class = overriding_consumer_class

                break

        consumer = consumer_class(self._url, route, callback, self._shared_stream, resumable, distributed, options)
        consumer.start()

        self._consumers.append(consumer)
