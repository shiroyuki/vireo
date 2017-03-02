from .core   import Core
from .helper import log

ASYNC_START = 1
SYNC_START  = 2


class UnknownRunningModeError(RuntimeError):
    """ Error for unknown running mode """


class Observer(Core):
    """ Event Observer """
    # def open(self, event_name, options = None, delegation_ttl = None):
    #     """ Prepare to observe an event.
    #
    #         :param str event_name:     the name of the queue
    #         :param str options:        the option for the driver
    #         :param int delegation_ttl: the TTL for the delegation*
    #
    #         To enable delegation, ``delegation_ttl`` must not be set to ``None`` or ``0`` or ``False``.
    #
    #         The delegation of the event happens when there exists no listener to that event. Then,
    #         the message will be transfered to the delegated event, which is similar to the given
    #         event name but suffixed with ``.delegated``. For instance, given an event name
    #         "outbound_mail_delivery", the delegated event name will be "outbound_mail_delivery.delegated".
    #
    #         To handle the delegated event, simply listen to the delegated event. For example,
    #         continuing from the previous example, you can write ``on('outbound_mail_delivery.delegated', lambda x: foo(x))``.
    #
    #         For example,
    #
    #         .. code-block:: python
    #
    #             app.open('foo')
    #
    #         .. note:: the delegation might change in the future to simplify the interface.
    #     """
    #     if delegation_ttl:
    #         log('debug', 'Preparing to observe event "{}" with delegation after {} seconds'.format(event_name, delegation_ttl / 1000))
    #
    #         self._driver.declare_queue_with_delegation(
    #             event_name,
    #             delegation_ttl,
    #             common_queue_options = options
    #         )
    #
    #         log('debug', 'Ready to observe event "{}" with delegation after {} seconds'.format(event_name, delegation_ttl / 1000))
    #
    #         return
    #
    #     log('debug', 'Preparing to observe event "{}"'.format(event_name))
    #
    #     self._driver.declare_queue(event_name, options or {})
    #
    #     log('debug', 'Ready to observe event "{}"'.format(event_name))
    #
    # def close(self, event_name, options = None):
    #     """ Clean up after observing an event.
    #
    #         :param str event_name: the name of the queue
    #         :param str options:    the option for the driver
    #     """
    #     log('debug', 'Preparing to stop observing event "{}"'.format(event_name))
    #
    #     self._driver.delete_queue(event_name, options or {})
    #
    #     log('debug', 'Event "{}" may not be observed properly at this point.'.format(event_name))

    def on(self, event_name, callback, resumable = False):
        """ Listen to an event with a callback function.

            :param str event_name: the name of the event
            :param callable callback: the callback callable
            :param bool resumable: the flag to indicate whether the event consumption can be resumed (as the data stream will never be deleted).

            The callback is a callable object, e.g., function, class method, lambda object, which
            takes only one parameter which is a JSON-decoded object.

            For example,

            .. code-block:: python

                def on_foo(self, message):
                    print('on_foo:', message)

                app.on('foo', on_foo)
                app.on('foo.lambda', lambda x: print('foo_lambda:', x))
        """
        self._driver.observe(event_name, callback, resumable, False)

    def on_broadcast(self, event_name, callback):
        """ Listen to an distributed event with a callback function.

            The callback is a callable object, e.g., function, class method, lambda object, which
            takes only one parameter which is a JSON-decoded object.

            For example,

            .. code-block:: python

                def on_foo(self, message):
                    print('on_foo:', message)

                app.on('foo', on_foo)
                app.on('foo.lambda', lambda x: print('foo_lambda:', x))
        """
        self._driver.observe(event_name, callback, False, True)

    def join(self, running_mode = SYNC_START):
        """ Start the observation

            There are two mode: synchronous (``vireo.observer.SYNC_START``) and asynchronous
            (``vireo.observer.ASYNC_START``) starts.

            .. code-block:: python

                app.start(ASYNC_START)
        """
        if running_mode == ASYNC_START:
            self._driver.setup_async_cleanup()

            return

        if running_mode == SYNC_START:
            self._driver.join()

            return

        raise UnknownRunningModeError(running_mode)

    def stop(self):
        """ Stop the observation

            .. code-block:: python

                app.stop()
        """
        self._driver.stop_consuming()
