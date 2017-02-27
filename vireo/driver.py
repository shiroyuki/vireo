import contextlib
import json
import logging
import threading
import time

from pika            import BlockingConnection, BasicProperties
from pika.connection import URLParameters
from pika.exceptions import ConnectionClosed, ChannelClosed

from .helper import fill_in_the_blank, log


def get_blocking_queue_connection(url):
    init_params = URLParameters(url)

    return BlockingConnection(init_params)


@contextlib.contextmanager
def active_connection(url):
    log('debug', 'Connecting')

    connection = get_blocking_queue_connection(url)
    channel    = connection.channel()

    log('debug', 'Connected and channel opened')

    log('debug', 'Yielding the opened channel')

    yield channel

    log('debug', 'Regained the opened channel')

    log('debug', 'Disconnecting')

    channel.close()
    connection.close()

    log('debug', 'Disconnected')


class SubscriptionNotAllowedError(RuntimeError):
    """ Subscription not allowed """


class Consumer(threading.Thread):
    def __init__(self, url, route, callback):
        super().__init__(daemon = True)

        self.url      = url
        self.route    = route
        self.callback = callback
        self._channel = None

    def run(self):
        with active_connection(self.url) as channel:
            self._channel = channel

            # Declare the callback wrapper for this route.
            def callback_wrapper(channel, method_frame, header_frame, body):
                log('debug', 'Method Frame: {}'.format(method_frame))
                log('debug', 'Header Frame: {}'.format(header_frame))
                log('debug', 'Body: {}'.format(header_frame))

                self.callback(json.loads(body.decode('utf8')))
                channel.basic_ack(delivery_tag = method_frame.delivery_tag)

            log('debug', 'Listening to {}'.format(self.route))

            channel.basic_consume(callback_wrapper, self.route)

            # NOTE there is a bug in start_consuming that prevents stop_consuming from cleanly
            #      stopping message consumption. The following is a hack suggested in StackOverflow.
            # channel.start_consuming()
            while channel._consumer_infos:
                channel.connection.process_data_events(time_limit=1)

            log('debug', 'Stopped listening to {}'.format(self.route))

    def stop(self):
        log('debug', 'Stopping listening to {}'.format(self.route))
        self._channel.stop_consuming()


class AsyncRabbitMQDriver(object):
    def __init__(self, url):
        self._url             = url
        self._async_listener  = None
        self._has_term_signal = False

    def declare_queue(self, queue_name, options):
        with active_connection(self._url) as channel:
            channel.queue_declare(queue = queue_name, **options)

    def delete_queue(self, queue_name, options):
        with active_connection(self._url) as channel:
            channel.queue_delete(queue = queue_name, **options)

    def declare_exchange(self, exchange_name, exchange_type, **kwargs):
        with active_connection(self._url) as channel:
            channel.exchange_declare(exchange = exchange_name, exchange_type = exchange_type, **kwargs)

    def start_consuming(self, route_to_callbacks = None):
        """ Asynchronously consume messages

            :param dict route_to_callbacks: the route-to-callback dictionary
        """
        if self._async_listener and self._async_listener.is_alive():
            raise SubscriptionNotAllowedError('Unable to consume messages as this driver is currently active.')

        self._async_listener = threading.Thread(
            target = self.synchronously_consume,
            kwargs = {
                'route_to_callbacks': route_to_callbacks,
            }
        )

        self._async_listener.start()

    def stop_consuming(self):
        self._has_term_signal = True

    def synchronously_consume(self, route_to_callbacks):
        """ Synchronously consume messages

            :param dict route_to_callbacks: the route-to-callback dictionary
        """
        route_listeners = []

        for route, callback in route_to_callbacks.items():
            route_listener = Consumer(self._url, route, callback)
            route_listener.start()
            route_listeners.append(route_listener)

        try:
            while not self._has_term_signal:
                time.sleep(1)
        except KeyboardInterrupt:
            log('info', 'SIGTERM received. Terminating all route listeners.')

        for route_listener in route_listeners:
            if not route_listener.is_alive():
                log('info', 'Route {}: Already stopped listening (not alive).'.format(route_listener.route))

                continue

            log('debug', 'Route {}: Sending the signal to stop listening.'.format(route_listener.route))
            route_listener.stop()
            log('debug', 'Route {}: Terminating the listener.'.format(route_listener.route))
            try:
                route_listener._stop()
            except AssertionError: # this is raised if the thread lock is still locked.
                log('warning', 'Route {}: Failed to stop properly'.format(route_listener.route))

            if not route_listener.is_alive():
                log('info', 'Route {}: Termination confirmed (killed)'.format(route_listener.route))

                continue

            log('debug', 'Route {}: Waiting the listener to join back to the parent thread.'.format(route_listener.route))
            route_listener.join()
            log('info', 'Route {}: Termination confirmed (joined).'.format(route_listener.route))

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
            log('debug', 'Declaring: route={}'.format(route))
            channel.queue_declare(queue = route, passive = True)
            log('debug', 'Declared: route={}'.format(route))

            log('debug', 'Publishing: route={} message={} options={}'.format(route, message, options))
            channel.basic_publish(**options)
            log('debug', 'Published: route={} message={} options={}'.format(route, message, options))

    def declare_queue_with_delegation(self, origin_queue_name, ttl, fallback_queue_name = None,
                                      common_queue_options = None, exchange_options = None):

        actual_fallback_queue_name = fallback_queue_name or '{}.delegated'.format(origin_queue_name)
        exchange_name              = 'fallback/{}/{}'.format(origin_queue_name, actual_fallback_queue_name)

        exchange_options     = exchange_options     or {}
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
            channel.exchange_declare(**exchange_options)

            self.declare_queue(actual_fallback_queue_name, fallback_queue_options)

            channel.queue_bind(
                queue    = actual_fallback_queue_name,
                exchange = exchange_name,
            )

            self.declare_queue(origin_queue_name, origin_queue_options)
