# coding: utf-8

from ..models import Rpc

from libreactor.basic_protocols import MessageReceiver


class RpcProtocol(MessageReceiver):

    def msg_received(self, msg):

        rpc = Rpc.from_json(msg.json())
        self.ctx.raft.dispatch_rpc(rpc)
