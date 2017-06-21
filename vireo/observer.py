from uuid import uuid4

from .core   import Core
from .helper import log
from .model  import RemoteSignal

ASYNC_START = 1
SYNC_START  = 2


class UnknownRunningModeError(RuntimeError):
    """ Error for unknown running mode """


class Observer(Core):
    """ Event Observer """
    def __init__(self, driver):
        Core.__init__(self, driver)

        self._identifier = str(uuid4())

        self._broadcast_event_to_observer_map = {}
        self._normal_event_to_observer_map    = {}

        log('debug', 'Observer ID: {}'.format(self.id))

    @property
    def id(self) -> str:
        """ Observer Identifier """
        return self._identifier

    def on(self, event_name, callback, resumable = False, simple_handling = True):
        """ Listen to an event with a callback function.

            :param str event_name: the name of the event
            :param callable callback: the callback callable
            :param bool resumable: the flag to indicate whether the event consumption can be resumed (as the data stream will never be deleted).
            :param bool simple_handling: the flag to instruct the code to return the content of the message, instead of returning the whole :class:`vireo.model.Message` object.

            The callback is a callable object, e.g., function, class method, lambda object, which
            takes only one parameter which is a JSON-decoded object.

            For example,

            .. code-block:: python

                def on_foo(self, message):
                    print('on_foo:', message)

                app.on('foo', on_foo)
                app.on('foo.lambda', lambda x: print('foo_lambda:', x))

            Here is an example for ``error_handler``.

            .. code-block:: Python

                def error_handler(consumer, exception):
                    ...
        """
        internal_observer = self._driver.observe(event_name, callback, resumable, False,
                                                 simple_handling = simple_handling,
                                                 controller_id = self.id)

        self._register_event_handler(self._normal_event_to_observer_map, event_name, internal_observer)

        return internal_observer

    def on_broadcast(self, event_name, callback, simple_handling = True):
        """ Listen to an distributed event with a callback function.

            :param str event_name: the name of the event
            :param callable callback: the callback callable
            :param bool simple_handling: the flag to instruct the code to return the content of the message, instead of returning the whole :class:`vireo.model.Message` object.

            The callback is a callable object, e.g., function, class method, lambda object, which
            takes only one parameter which is a JSON-decoded object.

            For example,

            .. code-block:: python

                def on_foo(self, message):
                    print('on_foo:', message)

                app.on('foo', on_foo)
                app.on('foo.lambda', lambda x: print('foo_lambda:', x))

            Here is an example for ``error_handler``.

            .. code-block:: Python

                def error_handler(consumer, exception):
                    ...
        """
        internal_observer = self._driver.observe(event_name, callback, False, True,
                                                 simple_handling = simple_handling,
                                                 controller_id = self.id)

        self._register_event_handler(self._broadcast_event_to_observer_map, event_name, internal_observer)

        return internal_observer

    def pause_on(self, event_name, remote_identifier = None):
        if not remote_identifier:
            self._remote_to_all_observers_on(self._normal_event_to_observer_map, event_name, 'pause')

            return

        self.emit(
            event_name,
            {
                'remote_signal' : RemoteSignal.PAUSE,
                'controller_id' : remote_identifier,
            }
        )

    def resume_on(self, event_name, remote_identifier = None):
        if not remote_identifier:
            self._remote_to_all_observers_on(self._normal_event_to_observer_map, event_name, 'resume')

            return

        self.emit(
            event_name,
            {
                'remote_signal' : RemoteSignal.RESUME,
                'controller_id' : remote_identifier,
            }
        )

    def pause_on_broadcast(self, event_name, remote_identifier = None):
        if not remote_identifier:
            self._remote_to_all_observers_on(self._broadcast_event_to_observer_map, event_name, 'pause')

            return

        self.broadcast(
            event_name,
            {
                'remote_signal' : RemoteSignal.PAUSE,
                'controller_id' : remote_identifier,
            }
        )

    def resume_on_broadcast(self, event_name, remote_identifier = None):
        if not remote_identifier:
            self._remote_to_all_observers_on(self._broadcast_event_to_observer_map, event_name, 'resume')

            return

        self.broadcast(
            event_name,
            {
                'remote_signal' : RemoteSignal.RESUME,
                'controller_id' : remote_identifier,
            }
        )

    def join(self, running_mode = SYNC_START):
        """ Wait for all handlers to stop.

            There are two mode: synchronous (``vireo.observer.SYNC_START``) and asynchronous
            (``vireo.observer.ASYNC_START``) joins.

            .. code-block:: python

                app.join(ASYNC_START)
        """
        if running_mode == ASYNC_START:
            self._driver.setup_async_cleanup()

            return

        if running_mode == SYNC_START:
            self._driver.join()

            return

        raise UnknownRunningModeError(running_mode)

    def stop(self):
        """ Send the signal to all handlers to stop observation.

            .. warning:: This method does not block the caller thread while waiting all handlers to stop.

            .. code-block:: python

                app.stop()
        """
        self._driver.stop_consuming()

    def _register_event_handler(self, observer_map, event_name, observer):
        if event_name not in observer_map:
            observer_map[event_name] = []

        observer_map[event_name].append(observer)

    def _find_all_event_handlers(self, observer_map, event_name, observer):
        if event_name not in observer_map:
            return []

        return observer_map[event_name]

    def _remote_to_all_observers_on(self, observer_map, event_name, remote_command):
        internal_observers = self._find_all_event_handlers(observer_map, event_name)

        for internal_observer in internal_observers:
            if not hasattr(internal_observer, remote_command):
                continue

            actual_remote_command = getattr(internal_observer, remote_command)

            if not callable(actual_remote_command):
                continue

            actual_remote_command()
