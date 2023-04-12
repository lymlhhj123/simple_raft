# coding: utf-8


class Rpc(object):

    def __init__(self, message, channel):
        """

        :param message:
        :param channel:
        """
        self.message = message
        self.channel = channel

    def reply(self, message):
        """

        :param message:
        :return:
        """
        self.channel.send_json(message.as_json())
