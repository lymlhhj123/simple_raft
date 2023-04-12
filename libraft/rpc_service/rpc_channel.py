# coding: utf-8

from .rpc import Rpc

from libreactor import MessageReceiver


class RpcChannel(MessageReceiver):

    def __init__(self):

        super(RpcChannel, self).__init__()

        self.raft = None

    def connection_established(self):
        """

        :return:
        """
        self.raft = self.ctx.raft

    def connection_made(self):
        """

        :return:
        """
        self.raft = self.ctx.raft

    def connection_closed(self):
        """

        :return:
        """
        self.raft = None

    def msg_received(self, msg):

        rpc = Rpc(msg, self)
        self.raft.dispatch_rpc(rpc)
