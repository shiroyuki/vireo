class Core(object):
    def __init__(self, driver):
        self._driver = driver

    def open(self, queue_name, **kwargs):
        self._driver.declare_queue(queue_name, **kwargs)

    def close(self, queue_name, **kwargs):
        self._driver.delete_queue(queue_name, **kwargs)

    def open_with_ttl(self, original_queue, ttl, fallback_queue = None):
        actual_fallback_queue = fallback_queue or '{}.fallback'.format(original_queue)
        exchange_name = 'fallback/{}/{}'.format(original_queue, actual_fallback_queue)

        self._driver.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

        self._driver.declare_queue(
            actual_fallback_queue,
            auto_delete = False,
        )

        self._driver.channel.queue_bind(
            queue=actual_fallback_queue,
            exchange=exchange_name
        )

        self._driver.declare_queue(
            original_queue,
            auto_delete = False,
            arguments = {
                'x-dead-letter-exchange': exchange_name,
                'x-dead-letter-routing-key': actual_fallback_queue,
                'x-message-ttl': ttl,
            }
        )
