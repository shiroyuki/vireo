import contextlib

from pika            import BlockingConnection
from pika.connection import URLParameters
from pika.exceptions import ConnectionClosed, ChannelClosed, IncompatibleProtocolError

from ...helper  import fill_in_the_blank, log
from .exception import NoConnectionError

SHARED_SIGNAL_CONNECTION_LOSS = 1
SHARED_TOPIC_EXCHANGE_NAME    = 'vireo_default_topic_r0'


def get_blocking_queue_connection(url):
    init_params = URLParameters(url)

    return BlockingConnection(init_params)


@contextlib.contextmanager
def active_connection(url, on_connect, on_disconnect):
    log('debug', 'Connecting')

    try:
        connection = get_blocking_queue_connection(url)
        channel    = connection.channel()

        if on_connect:
            on_connect()
    except IncompatibleProtocolError:
        if on_disconnect:
            on_disconnect()

        raise NoConnectionError('Incompatible Protocol')
    except ChannelClosed:
        if on_disconnect:
            on_disconnect()

        raise NoConnectionError('Failed to communicate while opening an active channel')
    except ConnectionClosed:
        if on_disconnect:
            on_disconnect()

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
