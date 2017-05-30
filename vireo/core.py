import threading

from .exception import NoConnectionError
from .helper    import log


class Core(object):
    def __init__(self, driver):
        self._driver = driver

    def emit(self, event_name, data = None, options = None, error_suppressed = True):
        """ Emit a message to a particular (shared) event.

            .. code-block:: python

                app.emit('security.alert.intrusion', {'ip': '127.0.0.1'})

        """
        log('debug', 'Publishing "{}" with {}'.format(event_name, data))

        try:
            self._driver.publish(event_name, data, options or {})
        except NoConnectionError as e:
            if error_suppressed:
                log('error', 'Failed to publish "{}" with {} ({})'.format(event_name, data, e))

                return

            raise NoConnectionError('Failed to emit an event {}.'.format(event_name))

        log('debug', 'Published "{}" with {}'.format(event_name, data))

    def broadcast(self, event_name, data = None, options = None, error_suppressed = True):
        """ Broadcast a message to a particular (distributed) event.

            .. code-block:: python

                app.broadcast('system.down', {'service_category': 'go_board'})

        """
        log('debug', 'Broadcasting "{}" with {}'.format(event_name, data))

        try:
            self._driver.broadcast(event_name, data, options or {})
        except NoConnectionError as e:
            if error_suppressed:
                log('error', 'Failed to broadcast "{}" with {} ({})'.format(event_name, data, e))

                return

            raise NoConnectionError('Failed to broadcast an event {}.'.format(event_name))

        log('debug', 'Broadcasted "{}" with {}'.format(event_name, data))

    async def emit_passively(self, event_name, data = None, options = None, error_suppressed = True):
        """ Emit (dispatch) an event asynchronously.

            This is similar to :method:`emit`.
        """
        self.emit(event_name, data, options, error_suppressed)

    async def broadcast_passively(self, event_name, data = None, options = None, error_suppressed = True):
        """ Broadcast an event asynchronously.

            This is similar to :method:`broadcast`.
        """
        self.broadcast(event_name, data, options, error_suppressed)
