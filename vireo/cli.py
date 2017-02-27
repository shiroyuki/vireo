import json
import logging

from gallium.interface import ICommand

from .core     import Core
from .driver   import AsyncRabbitMQDriver
from .observer import Observer, SYNC_START
from .helper   import prepare_logger


class Server(ICommand):
    """ Run the sample """
    def identifier(self):
        return 'sample.observe'

    def define(self, parser):
        parser.add_argument(
            '--debug',
            '-d',
            action = 'store_true'
        )

        parser.add_argument(
            '--bind-url',
            '-b',
            default='amqp://guest:guest@172.17.0.1:5672/%2F'
        )

    def execute(self, args):
        # logging.basicConfig(level = logging.DEBUG if args.debug else logging.INFO)
        prepare_logger(logging.DEBUG if args.debug else logging.INFO)

        driver = AsyncRabbitMQDriver(args.bind_url)
        vireo  = Observer(driver)

        vireo.open('vireo.sample.primary', delegation_ttl = 5000)
        vireo.open('vireo.sample.secondary')
        vireo.open('vireo.sample.direct')

        vireo.on('vireo.sample.direct',            lambda x: print('vireo.sample.direct: {}'.format(x)))
        vireo.on('vireo.sample.secondary',         lambda x: print('vireo.sample.secondary: {}'.format(x)))
        vireo.on('vireo.sample.primary.delegated', lambda x: print('vireo.sample.primary.delegated: {}'.format(x)))

        try:
            vireo.start(SYNC_START)
        except KeyboardInterrupt:
            vireo.stop()


class EventCore(ICommand):
    """ Emit an event """
    def identifier(self):
        return 'event.emit'

    def define(self, parser):
        parser.add_argument(
            '--debug',
            '-d',
            action = 'store_true'
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

        parser.add_argument(
            '--bind-url',
            '-b',
            default='amqp://guest:guest@172.17.0.1:5672/%2F'
        )

    def execute(self, args):
        prepare_logger(logging.DEBUG if args.debug else logging.INFO)

        driver = AsyncRabbitMQDriver(args.bind_url)
        vireo  = Core(driver)

        vireo.emit(args.event_name, json.loads(args.event_data) if args.event_data else None)
