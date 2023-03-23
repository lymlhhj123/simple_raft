# coding: utf-8

from libreactor.internet import TcpClient
from libreactor.context import ClientContext

from .rpc_protocol import RpcProtocol


class RpcContext(ClientContext):

    protocol_cls = RpcProtocol

    def __init__(self, raft):

        super(RpcContext, self).__init__()

        self.raft = raft


class RpcClient(object):

    def __init__(self, host, port, ev, raft, on_established):

        self.host = host
        self.port = port
        self.ev = ev

        ctx = RpcContext(raft)
        ctx.set_established_callback(on_established)

        self.tcp_server = TcpClient(host, port, ev, ctx, auto_reconnect=True)
