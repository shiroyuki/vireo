import logging
import pprint

from gallium.interface import ICommand

from .drivers.rabbitmq import Driver
from .observer         import Observer, SYNC_START
from .helper           import prepare_logger


class SampleObserveWithDefaultOptions(ICommand):
    """ Run the sample observer with default options """
    def identifier(self):
        return 'sample.observe.default'

    def define(self, parser):
        parser.add_argument(
            '--debug',
            '-d',
            action = 'store_true'
        )

        parser.add_argument(
            '--bind-url',
            '-b',
            default='amqp://guest:guest@127.0.0.1:5672/%2F'
        )

    def execute(self, args):
        logger = prepare_logger(logging.DEBUG if args.debug else logging.INFO)

        def handle_driver_event(level, label, c):
            if not c or not hasattr(c, 'route') or not hasattr(c, 'queue_name'):
                getattr(logger, level.lower())(label)

                return

            getattr(logger, level.lower())('{} ({}): {}'.format(
                c.route,
                c.queue_name,
                label,
            ))

        driver = Driver(
            args.bind_url,
            unlimited_retries = True,
            on_connect        = lambda c = None:           handle_driver_event('info',  '--><-- CONNECTED',    c),
            on_disconnect     = lambda c = None:           handle_driver_event('error', '-x--x- DISCONNECTED', c),
            on_error          = lambda c = None, e = None: handle_driver_event('error', '-->-x- ERROR',        c),
        )

        service = Observer(driver)

        # In this example, delegation is disabled.
        # vireo.open('vireo.sample.primary', delegation_ttl = 5000)
        # vireo.on('vireo.sample.primary.delegated', lambda x: print('vireo.sample.primary.delegated: {}'.format(x)))

        def wrapper(label, data):
            print('[SAMPLE] {}:'.format(label))

            pprint.pprint(data, indent = 2)

        service.on('vireo.sample.direct',           lambda x: wrapper('vireo.sample.direct',    x))
        service.on('vireo.sample.secondary',        lambda x: wrapper('vireo.sample.secondary', x))
        service.on('vireo.sample.direct.resumable', lambda x: wrapper('vireo.sample.direct',    x), resumable = True)

        # With custom TOPIC exchange
        service.on(
            'vireo.sample.custom_topic_exchange.route_1',
            lambda x: wrapper('vireo.sample.custom_topic_exchange', x),
            options = {
                'exchange': {
                    'name': 'vireo_sample_topic_exchange',
                    'type': 'topic',
                }
            }
        )

        service.on(
            'vireo.sample.custom_topic_exchange.route_2',
            lambda x: wrapper('vireo.sample.custom_topic_exchange', x),
            options = {
                'exchange': {
                    'name': 'vireo_sample_topic_exchange',
                    'type': 'topic',
                }
            }
        )

        # With custom FANOUT exchange
        service.on(
            'vireo.sample.custom_fanout_exchange_1',
            lambda x: wrapper('vireo.sample.custom_fanout_exchange_1', x),
            options = {
                'exchange': {
                    'name': 'vireo_sample_fanout_exchange',
                    'type': 'fanout',
                }
            }
        )

        service.on(
            'vireo.sample.custom_fanout_exchange_2',
            lambda x: wrapper('vireo.sample.custom_fanout_exchange_2', x),
            options = {
                'exchange': {
                    'name': 'vireo_sample_fanout_exchange',
                    'type': 'fanout',
                }
            }
        )

        # With handling errors
        def error_demo(x):
            if 'e' in x:
                raise RuntimeError('Intentional Error')

            wrapper('vireo.sample.error', x)

        service.on('vireo.sample.error', error_demo)


        service.on_broadcast('vireo.sample.broadcast.one', lambda x: wrapper('vireo.sample.broadcast.one', x))
        service.on_broadcast('vireo.sample.broadcast.two', lambda x: wrapper('vireo.sample.broadcast.two', x))

        service.join(SYNC_START)


class SampleObserveWithCustomOptions(ICommand):
    """ Run the sample observer with custom options """
    def identifier(self):
        return 'sample.observe.custom'

    def define(self, parser):
        parser.add_argument(
            '--debug',
            '-d',
            action = 'store_true'
        )

        parser.add_argument(
            '--bind-url',
            '-b',
            default='amqp://guest:guest@127.0.0.1:5672/%2F'
        )

    def execute(self, args):
        logger = prepare_logger(logging.DEBUG if args.debug else logging.INFO)

        def handle_driver_event(level, label, c):
            if not c or not hasattr(c, 'route') or not hasattr(c, 'queue_name'):
                getattr(logger, level.lower())(label)

                return

            getattr(logger, level.lower())('{} ({}): {}'.format(
                c.route,
                c.queue_name,
                label,
            ))

        driver = Driver(
            args.bind_url,
            unlimited_retries = True,
            on_connect        = lambda c = None:           handle_driver_event('info',  '--><-- CONNECTED',    c),
            on_disconnect     = lambda c = None:           handle_driver_event('error', '-x--x- DISCONNECTED', c),
            on_error          = lambda c = None, e = None: handle_driver_event('error', '-->-x- ERROR',        c),
            default_consuming_shared_queue_options = {
                'exchange': {
                    'name': 'vireo_sample_custom_default_exchange',
                    'type': 'topic',
                },
            },
        )

        service = Observer(driver)

        def wrapper(label, data):
            print('[SAMPLE] {}:'.format(label))

            pprint.pprint(data, indent = 2)

        # With custom TOPIC exchange
        service.on(
            'vireo.sample_custom.route_1',
            lambda x: wrapper('vireo.sample_custom.route_1', x),
        )

        service.on(
            'vireo.sample_custom.route_2',
            lambda x: wrapper('vireo.sample_custom.route_2', x),
        )

        service.join(SYNC_START)
