# coding: utf-8

import logging

import simple_reactor

from .message import Message
from . import rpc_protocol

logger = logging.getLogger()


class TcpChannel(object):

    def __init__(self, local_addr, cluster_members, loop, raft):

        self._local_addr = local_addr
        self._cluster_members = cluster_members
        self._loop = loop
        self._raft = raft

        self._lock = self._loop.create_Lock()
        self._protocol_map = {}

        # write queue
        self._producer_q = self._loop.create_fifo_queue()

        # read queue
        self._consumer_q = self._loop.create_fifo_queue()

    def init(self):
        """init tcp channel"""
        self._loop.create_task(self._listen())

        for addr in self._cluster_members:
            if addr == self._local_addr:
                continue

            self._loop.create_task(self._connect(addr))

    async def _listen(self):
        """listen tcp on local addr"""
        ip, port = self._local_addr.rsplit(":", 1)
        # todo: set tcp protocol factory
        await self._loop.listen_tcp(ip, port)

    async def _connect(self, addr):
        """established connection to remote addr"""
        ip, port = addr.rsplit(":", 1)

        while True:
            # todo: set tcp protocol factory
            try:
                proto = await self._loop.connect_tcp(ip, port)
            except Exception as e:
                logger.exception(f"Failed to connect {ip}:{port}, %s", e)
                await simple_reactor.sleep(1)
            else:
                await self._set_protocol(addr, proto)

                # wait connection closed
                await proto.wait()

                await self._set_protocol(addr, None)

                del proto

    async def _get_protocol(self, addr):
        """get tcp protocol by addr"""
        async with self._lock:
            return self._protocol_map.get(addr)

    async def _set_protocol(self, addr, proto):
        """set tcp protocol by addr"""
        async with self._lock:
            self._protocol_map[addr] = proto

    async def send_request_vote_request(self, addr, data):
        """send request vote to target addr"""
        headers = self._raft.get_default_rpc_headers()

        m = Message.create_request_vote_req(headers, data)

        await self.send(addr, m)

    async def send_append_entries_request(self, addr, data):
        """send append entries to target addr"""
        headers = self._raft.get_default_rpc_headers()

        m = Message.create_append_entries_req(headers, data)

        await self.send(addr, m)

    async def send_request_vote_resp(self, addr, data):
        """send request vote resp to target addr"""
        headers = self._raft.get_default_rpc_headers()

        m = Message.create_request_vote_resp(headers, data)

        await self.send(addr, m)

    async def send_append_entries_resp(self, addr, data):
        """send append entries resp to target addr"""
        headers = self._raft.get_default_rpc_headers()

        m = Message.create_append_entries_resp(headers, data)

        await self.send(addr, m)

    async def send(self, addr, message):
        """send message to addr"""
        await self._producer_q.put((addr, message))

    async def run_sender(self):
        """long-running coroutine, get message from queue and send it"""
        while True:
            addr, message = await self._producer_q.get()
            proto = await self._get_protocol(addr)
            if not proto:
                continue

            try:
                await proto.send_message(message)
            except Exception as e:
                logger.exception(f"failed to send {message} to {addr}, err: {e}")
