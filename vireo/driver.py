import threading

from pika            import BlockingConnection
from pika.connection import URLParameters
from pika.exceptions import ConnectionClosed, ChannelClosed


def get_blocking_queue_connection(url):
    init_params = URLParameters(url)

    return BlockingConnection(init_params)


class SubscriptionNotAllowedError(RuntimeError):
    """ Subscription not allowed """


class AsyncRabbitMQDriver(object):
    def __init__(self, url):
        self._url            = url
        self._async_listener = None
        self._connection     = None
        self._channel        = None

    @property
    def connection(self):
        if not self._connection:
            self._connection = get_blocking_queue_connection(self._url)

        return self._connection

    @property
    def channel(self):
        if not self._channel or self._channel.is_closed:
            self._channel = self.connection.channel()

        return self._channel

    def disconnect(self):
        if not self._connection:
            return

        self._connection.close()

        self._connection = None

    def declare_queue(self, queue_name, nowait=False, **kwargs):
        self.channel.queue_declare(queue=queue_name, **kwargs)

    def delete_queue(self, queue_name, **kwargs):
        self.channel.queue_delete(queue=queue_name, **kwargs)

    def declare_exchange(self, exchange_name, exchange_type, **kwargs):
        self.channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type, **kwargs)

    def start_consuming(self, route_to_callbacks : dict = None):
        if self._async_listener and self._async_listener.is_alive():
            raise SubscriptionNotAllowedError('Unable to consume messages as this driver is currently active.')

        self._async_listener = threading.Thread(
            target = self.synchronously_consume,
            kwargs = {
                'route_to_callbacks': route_to_callbacks,
            }
        )

    def stop_consuming(self):
        if not self._async_listener or not self._async_listener.is_alive():
            return

        self.channel.stop_consuming()
        self.connection.close()

    def synchronously_consume(self, route_to_callbacks : dict):
        if self.connection.is_closed:
            return self.connection.open()

        for route, callback in route_to_callbacks.items():
            def callback_wrapper(channel, method_frame, header_frame, body):
                try:
                    print(header_frame)

                    callback(body)

                    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                except Exception as e:
                    pass

            try:
                self.channel.basic_consume(callback_wrapper, route)
            except ChannelClosed as e:
                self._channel = self.connection.channel()

                self.declare_queue(route, durable=False, exclusive=False, auto_delete=True)

                self.channel.basic_consume(callback_wrapper, route)

        self.channel.start_consuming()
