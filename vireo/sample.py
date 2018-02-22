import json
import logging
import pprint
import sys
import time

from gallium.interface import ICommand

from .drivers.rabbitmq import Driver
from .observer         import Core, Observer, SYNC_START
from .helper           import prepare_logger

logger = prepare_logger(logging.DEBUG if '-d' in sys.argv else logging.INFO)


def handle_driver_event(level, label, c, summary = None):
    if not c or not hasattr(c, 'route') or not hasattr(c, 'queue_name'):
        getattr(logger, level.lower())(label)

        return

    getattr(logger, level.lower())('{} ({}): {}'.format(
        c.route,
        c.queue_name,
        label,
    ))


def on_connect(consumer = None, controller_id = None, route = None, queue_name = None, summary = None):
    handle_driver_event('info',  '<-------> CONNECTED', consumer)

def on_disconnect(consumer = None, controller_id = None, route = None, queue_name = None, summary = None):
    handle_driver_event('error', '---/ /--- DISCONNECTED', consumer)

def make_error_handler(exception, consumer = None, controller_id = None, route = None, queue_name = None, summary = None):
    handle_driver_event('error', '<--(!)--> ERROR', consumer)


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
        driver = Driver(
            args.bind_url,
            unlimited_retries = True,
            on_connect        = on_connect,
            on_disconnect     = on_disconnect,
            on_error          = make_error_handler,
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
        driver = Driver(
            args.bind_url,
            unlimited_retries = True,
            on_connect        = on_connect,
            on_disconnect     = on_disconnect,
            on_error          = make_error_handler,
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


class SamplePublishWithCustomOptions(ICommand):
    """ Run the sample observer with custom options """
    def identifier(self):
        return 'sample.publish.custom'

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

        parser.add_argument(
            'event_name',
            help = 'The name of the event (e.g., "sample.primary")'
        )

        parser.add_argument(
            'event_data',
            help  = 'The JSON-compatible string data of the event',
            nargs = '?'
        )

    def execute(self, args):
        prepare_logger(logging.DEBUG if args.debug else logging.INFO)

        driver = Driver(
            args.bind_url,
            default_publishing_options = {
                'exchange': 'vireo_sample_custom_default_exchange',
            },
        )

        service = Core(driver)

        service.emit(args.event_name, json.loads(args.event_data) if args.event_data else None)


class SampleObserveWithOneQueue(ICommand):
    """ Run the sample observer with default options """
    def identifier(self):
        return 'sample.observe.single'

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
        driver = Driver(
            args.bind_url,
            unlimited_retries = False,
            on_connect        = on_connect,
            on_disconnect     = on_disconnect,
            on_error          = make_error_handler,
        )

        def handler(data):
            print('[SAMPLE] Begin')

            print('[SAMPLE] Received: {}'.format(pprint.pformat(data, indent = 2)))

            if isinstance(data, dict) and 'sleep' in data:
                print('[SAMPLE] Sleeping')

                time.sleep(data.get('sleep'))

                print('[SAMPLE] Resumed')

            print('[SAMPLE] End')

        service = Observer(driver)
        service.on('vireo.sample.single', handler, resumable = True, max_retries = 30)
        service.join(SYNC_START)
