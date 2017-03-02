import json
import logging

from gallium.interface import ICommand

from .core             import Core
from .drivers.rabbitmq import Driver, NoConnectionError
from .observer         import Observer, SYNC_START
from .helper           import prepare_logger


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

        driver  = Driver(args.bind_url)
        service = Observer(driver)

        try:
            # In this example, delegation is disabled.
            # vireo.open('vireo.sample.primary', delegation_ttl = 5000)
            # vireo.on('vireo.sample.primary.delegated', lambda x: print('vireo.sample.primary.delegated: {}'.format(x)))

            service.on('vireo.sample.direct',           lambda x: print('vireo.sample.direct: {}'.format(x)))
            service.on('vireo.sample.secondary',        lambda x: print('vireo.sample.secondary: {}'.format(x)))
            service.on('vireo.sample.direct.resumable', lambda x: print('vireo.sample.direct: {}'.format(x)), resumable = True)

            service.join(SYNC_START)
        except KeyboardInterrupt:
            service.stop()


class EventEmitter(ICommand):
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

        driver  = Driver(args.bind_url)
        service = Core(driver)

        service.emit(args.event_name, json.loads(args.event_data) if args.event_data else None)

class EventBroadcaster(ICommand):
    """ Broadcast an event """
    def identifier(self):
        return 'event.broadcast'

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

        driver  = Driver(args.bind_url)
        service = Core(driver)

        service.broadcast(args.event_name, json.loads(args.event_data) if args.event_data else None)
