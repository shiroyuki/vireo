from .core import Core

ASYNC_START = 1
SYNC_START  = 2


class UnknownRunningModeError(RuntimeError):
    """ Error for unknown running mode """


class Server(Core):
    def __init__(self, driver):
        super().__init__(driver)

        self._event_to_callbacks = {}

    def on(self, event_name, callback):
        self._event_to_callbacks[event_name] = callback

    def start(self, running_mode = SYNC_START):
        if running_mode == ASYNC_START:
            self._driver.start_consuming(self._event_to_callbacks)

            return

        if running_mode == SYNC_START:
            self._driver.synchronously_consume(self._event_to_callbacks)

            return

        raise UnknownRunningModeError(running_mode)

    def stop(self):
        self._driver.stop_consuming()
