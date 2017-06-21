import json
import logging
import pprint
import sys

from gallium.interface import ICommand

from .core             import Core
from .drivers.rabbitmq import Driver, NoConnectionError
from .model            import RemoteSignal
from .observer         import Observer, SYNC_START
from .helper           import prepare_logger


class SampleObserve(ICommand):
    """ Run the sample observer. """
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
            on_connect        = lambda c = None:           handle_driver_event('info', '--><-- CONNECTED',     c),
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

        def error_demo(x):
            if 'e' in x:
                raise RuntimeError('Intentional Error')

            wrapper('vireo.sample.error', x)

        service.on('vireo.sample.error', error_demo)


        service.on_broadcast('vireo.sample.broadcast.one', lambda x: wrapper('vireo.sample.broadcast.one', x))
        service.on_broadcast('vireo.sample.broadcast.two', lambda x: wrapper('vireo.sample.broadcast.two', x))

        service.join(SYNC_START)


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


class ObserverRemoteControl(ICommand):
    """ Remotely control any observers on a particular event """
    def identifier(self):
        return 'observer'

    def define(self, parser):
        parser.add_argument(
            '--debug',
            '-d',
            action = 'store_true'
        )

        parser.add_argument(
            '--broadcast',
            action = 'store_true'
        )

        parser.add_argument(
            '--bind-url',
            '-b',
            default='amqp://guest:guest@172.17.0.1:5672/%2F'
        )

        parser.add_argument(
            'remote_id',
            help = 'UUID of the Observer (AKA Controller ID)'
        )

        parser.add_argument(
            'remote_command',
            help = 'ping, pause or resume'
        )

        parser.add_argument(
            'event_name',
            help = 'The name of the event (e.g., "sample.primary")'
        )

    def execute(self, args):
        prepare_logger(logging.DEBUG if args.debug else logging.INFO)

        driver  = Driver(args.bind_url)
        service = Core(driver)

        remote_command = args.remote_command.upper()

        if not hasattr(RemoteSignal, remote_command):
            raise RuntimeError('Unknown remote command')

        if args.broadcast:
            service.broadcast(
                args.event_name, {
                    'remote_signal': getattr(RemoteSignal, remote_command),
                    'controller_id': args.remote_id,
                }
            )

            return

        service.emit(
            args.event_name, {
                'remote_signal': getattr(RemoteSignal, remote_command),
                'controller_id': args.remote_id,
            }
        )


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
