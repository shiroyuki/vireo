_remote_command = lambda magic_word: 'remote:{}'.format(magic_word)

class Message(object):
    def __init__(self, content, additional = None):
        self.__content    = content
        self.__additional = additional

    @property
    def content(self):
        return self.__content

    @property
    def additional(self):
        return self.__additional

class RemoteSignal(object):
    PING   = _remote_command('ping')
    PAUSE  = _remote_command('pause')
    RESUME = _remote_command('resume')
