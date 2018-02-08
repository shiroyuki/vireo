import contextlib
import traceback
from urllib.parse import urlparse, unquote_plus

from amqp.connection import Connection

from vireo.helper  import fill_in_the_blank, log
from vireo.exception import NoConnectionError, InvalidURLError

SHARED_SIGNAL_CONNECTION_LOSS = 1
SHARED_DIRECT_EXCHANGE_NAME   = 'vireo_default_direct_r0'
SHARED_TOPIC_EXCHANGE_NAME    = 'vireo_default_topic_r0'


def make_connection(url):
    url_component = urlparse(url)

    if not url_component.scheme in ('amqp', 'amqps'):
        raise UnrecognizedProtocolError(url_component.scheme)

    virtual_host = '/'

    if url_component.path and len(url_component.path) > 1:
        virtual_host = unquote_plus(url_component.path[1:])

    return Connection(
        host         = '{hostname}:{port}'.format(hostname = url_component.hostname, port = url_component.port),
        userid       = url_component.username,
        password     = url_component.password,
        virtual_host = virtual_host,
        ssl          = url_component.scheme == 'amqps',
    )


@contextlib.contextmanager
def active_connection(url, on_connect, on_disconnect, on_error):
    log('debug', '[active_connection] New active connection to {}'.format(url))

    try:
        connection = make_connection(url)
        connection.connect()

        channel = connection.channel()

        if on_connect:
            on_connect()
    except Exception as e:
        if on_error:
            on_error(e, summary = summary)

        __raise_no_connection_error(
            'Failed to communicate while opening a channel ({}: {})'.format(type(e).__name__, e),
            on_disconnect,
        )

        raise NoConnectionError(summary)

    yield channel

    try:
        channel.close()
        connection.close()

        log('debug', '[active_connection] disconnected')
    except Exception as e:
        log('warning', '[active_connection] Unexpectedly disconnected before manual disconnection. ({})'.format(e))

        # bypassed if the connection is no longer available.

def __raise_no_connection_error(summary, on_disconnect):
    if on_disconnect:
        on_disconnect(summary = summary)

    raise NoConnectionError(summary)
