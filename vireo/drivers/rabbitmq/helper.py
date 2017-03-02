import contextlib

from pika            import BlockingConnection
from pika.connection import URLParameters
from pika.exceptions import ConnectionClosed, ChannelClosed

from ...helper import fill_in_the_blank, log

SHARED_SIGNAL_CONNECTION_LOSS = 1
SHARED_TOPIC_EXCHANGE_NAME    = 'vireo_default_topic_r0'


def get_blocking_queue_connection(url):
    init_params = URLParameters(url)

    return BlockingConnection(init_params)


@contextlib.contextmanager
def active_connection(url):
    log('debug', 'Connecting')

    try:
        connection = get_blocking_queue_connection(url)
        channel    = connection.channel()
    except ChannelClosed:
        raise NoConnectionError('Failed to communicate while opening an active channel')
    except ConnectionClosed:
        raise NoConnectionError('Failed to connect while opening an active connection')

    log('debug', 'Connected and channel opened')

    log('debug', 'Yielding the opened channel')

    yield channel

    log('debug', 'Regained the opened channel')

    log('debug', 'Disconnecting')

    try:
        channel.close()
        connection.close()

        log('debug', 'Disconnected')
    except ChannelClosed:
        log('warning', 'Already closed the channel') # bypassed if the connection is no longer available.
    except ConnectionClosed:
        log('warning', 'Already disconnected') # bypassed if the connection is no longer available.