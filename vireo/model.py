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
