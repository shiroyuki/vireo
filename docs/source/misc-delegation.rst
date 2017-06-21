Sample code for dealing message delegation with TTL
###################################################

.. code-block:: python

    def open(self, event_name, options = None, delegation_ttl = None):
        """ Prepare to observe an event.

            :param str event_name:     the name of the queue
            :param str options:        the option for the driver
            :param int delegation_ttl: the TTL for the delegation*

            To enable delegation, ``delegation_ttl`` must not be set to ``None`` or ``0`` or ``False``.

            The delegation of the event happens when there exists no listener to that event. Then,
            the message will be transfered to the delegated event, which is similar to the given
            event name but suffixed with ``.delegated``. For instance, given an event name
            "outbound_mail_delivery", the delegated event name will be "outbound_mail_delivery.delegated".

            To handle the delegated event, simply listen to the delegated event. For example,
            continuing from the previous example, you can write ``on('outbound_mail_delivery.delegated', lambda x: foo(x))``.

            For example,

            .. code-block:: python

                app.open('foo')

            .. note:: the delegation might change in the future to simplify the interface.
        """
        if delegation_ttl:
            log('debug', 'Preparing to observe event "{}" with delegation after {} seconds'.format(event_name, delegation_ttl / 1000))

            self._driver.declare_queue_with_delegation(
                event_name,
                delegation_ttl,
                common_queue_options = options
            )

            log('debug', 'Ready to observe event "{}" with delegation after {} seconds'.format(event_name, delegation_ttl / 1000))

            return

        log('debug', 'Preparing to observe event "{}"'.format(event_name))

        self._driver.declare_queue(event_name, options or {})

        log('debug', 'Ready to observe event "{}"'.format(event_name))
