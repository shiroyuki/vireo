import json
import logging

from gallium.interface import ICommand

from .core         import Core
from .drivers.amqp import Driver
from .model        import RemoteSignal
from .observer     import Observer
from .helper       import prepare_logger


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
            '--exchange',
            '-e',
            required = False,
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
            default='amqp://guest:guest@127.0.0.1:5672/%2F'
        )

    def execute(self, args):
        options = {}

        if args.exchange:
            options['exchange'] = args.exchange

        prepare_logger(logging.DEBUG if args.debug else logging.INFO)

        driver  = Driver(args.bind_url)
        service = Core(driver)

        service.emit(
            args.event_name,
            json.loads(args.event_data) if args.event_data else None,
            options,
            error_suppressed = False,
        )


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
            default='amqp://guest:guest@127.0.0.1:5672/%2F'
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
                args.event_name,
                {
                    'remote_signal': getattr(RemoteSignal, remote_command),
                    'controller_id': args.remote_id,
                },
                error_suppressed = False,
            )

            return

        service.emit(
            args.event_name,
            {
                'remote_signal': getattr(RemoteSignal, remote_command),
                'controller_id': args.remote_id,
            },
            error_suppressed = False,
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
            default='amqp://guest:guest@127.0.0.1:5672/%2F'
        )

    def execute(self, args):
        prepare_logger(logging.DEBUG if args.debug else logging.INFO)

        driver  = Driver(args.bind_url)
        service = Core(driver)

        service.broadcast(
            args.event_name,
            json.loads(args.event_data) if args.event_data else None,
            error_suppressed = False,
        )
