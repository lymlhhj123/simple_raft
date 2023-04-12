# coding: utf-8

from libreactor.internet import TcpServer
from libreactor.context import ServerContext

from .rpc_channel import RpcProtocol


class RpcContext(ServerContext):

    protocol_cls = RpcProtocol

    def __init__(self, raft):

        super(RpcContext, self).__init__()

        self.raft = raft


class RpcServer(object):

    def __init__(self, port, ev, raft):

        self.ev = ev
        self.port = port

        self.ctx = RpcContext(raft)
        self.tcp_server = TcpServer(port, ev, self.ctx)
