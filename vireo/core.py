from .helper import log


class Core(object):
    def __init__(self, driver):
        self._driver = driver

    def open(self, queue_name, options = None, delegation_ttl = None):
        """ Define a queue

            :param str queue_name:     the name of the queue
            :param str options:        the option of the queue (and the fallback queue)
            :param int delegation_ttl: the TTL for the delegation

            If ``delegation_ttl`` is ``None``, the delegation will not be enabled. Otherwise, it will be enabled.
        """
        if delegation_ttl:
            log('debug', 'Declaring queue {} with delegation after {} seconds'.format(queue_name, delegation_ttl / 1000))

            self._driver.declare_queue_with_delegation(
                queue_name,
                delegation_ttl,
                common_queue_options = options
            )

            log('debug', 'Declared queue {} with delegation after {} seconds'.format(queue_name, delegation_ttl / 1000))

            return

        log('debug', 'Declaring queue {}'.format(queue_name))

        self._driver.declare_queue(queue_name, options or {})

        log('debug', 'Declared queue {}'.format(queue_name))

    def close(self, queue_name, options = None):
        log('debug', 'Closing queue {}'.format(queue_name))

        self._driver.delete_queue(queue_name, options or {})

        log('debug', 'Closed queue {}'.format(queue_name))

    def emit(self, event_name, data = None, options = None):
        log('debug', 'Publishing "{}" with {}'.format(event_name, data))

        self._driver.publish(event_name, data, options or {})

        log('debug', 'Published "{}" with {}'.format(event_name, data))
