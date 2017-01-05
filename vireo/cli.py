from gallium.interface import ICommand

from .server import Server as VireoServer
from .server import SYNC_START
from .driver import AsyncRabbitMQDriver


class Server(ICommand):
    def identifier(self):
        return 'server'

    def define(self, parser):
        parser.add_argument(
            '--bind-url',
            '-b',
            default='amqp://guest:guest@172.17.0.1:5672/%2F'
        )

    def execute(self, args):
        driver = AsyncRabbitMQDriver(args.bind_url)
        server = VireoServer(driver)

        server.open_with_ttl('primary', 5000, 'secondary')
        server.on('secondary', lambda x: print('secondary: {}'.format(x)))

        try:
            server.start(SYNC_START)
        except KeyboardInterrupt:
            server.stop()
