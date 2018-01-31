import contextlib
import traceback

from pika            import BlockingConnection
from pika.connection import URLParameters
from pika.exceptions import ConnectionClosed, ChannelClosed, IncompatibleProtocolError

from ...helper  import fill_in_the_blank, log
from .exception import NoConnectionError

SHARED_SIGNAL_CONNECTION_LOSS = 1
SHARED_DIRECT_EXCHANGE_NAME   = 'vireo_default_direct_r0'
SHARED_TOPIC_EXCHANGE_NAME    = 'vireo_default_topic_r0'


def get_blocking_queue_connection(url):
    init_params = URLParameters(url)

    return BlockingConnection(init_params)


@contextlib.contextmanager
def active_connection(url, on_connect, on_disconnect, on_error):
    log('debug', '[active_connection] New active connection to {}'.format(url))

    try:
        connection = get_blocking_queue_connection(url)
        channel    = connection.channel()

        if on_connect:
            on_connect()
    except IncompatibleProtocolError as e:
        if on_error:
            on_error(e, summary = summary)

        raise NoConnectionError(summary)
    except ChannelClosed as e:
        __raise_no_connection_error(
            'Failed to communicate while opening an active channel ({}: {})'.format(type(e).__name__, e),
            on_disconnect,
        )
    except ConnectionClosed as e:
        __raise_no_connection_error(
            'Failed to connect while opening an active connection ({}: {})'.format(type(e).__name__, e),
            on_disconnect,
        )

    yield channel

    try:
        channel.close()
        connection.close()

        log('debug', '[active_connection] disconnected')
    except ChannelClosed as e:
        log('warning', '[active_connection] Unexpectedly closed the channel. ({})'.format(e))

        # bypassed if the connection is no longer available.
    except ConnectionClosed as e:
        log('warning', '[active_connection] Unexpectedly disconnected. ({})'.format(e))

        # bypassed if the connection is no longer available.

def __raise_no_connection_error(summary, on_disconnect):
    if on_disconnect:
        on_disconnect(summary = summary)

    raise NoConnectionError(summary)
