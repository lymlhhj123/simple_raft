# coding: utf-8

from rpc_client import RpcClient
from .rpc_server import RpcServer


class RpcService(object):

    def __init__(self, port, ev, raft):

        self.raft = raft

        self.protocol_map = {}

        self.rpc_server = RpcServer(port, ev, raft)

    def boostrap(self):
        """

        :return:
        """
        self.raft.members()

        # todo: make connection to other rpc server

    def get_protocol(self, addr):
        """

        :param addr:
        :return:
        """
        return self.protocol_map.get(addr)

    def add_protocol(self, addr, protocol):
        """

        :return:
        """
        self.protocol_map[addr] = protocol

    def remove_protocol(self, addr):
        """

        :param addr:
        :return:
        """
        self.protocol_map.pop(addr, None)

    def send_rpc(self, dst, rpc):
        """

        :param dst:
        :param rpc:
        :return:
        """
        protocol = self.get_protocol(dst)
        if not protocol:
            return

        protocol.send_json(rpc.as_json())
