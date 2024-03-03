# coding: utf-8

import json
import collections

from simple_reactor.protocols import IOStream


class RpcProtocol(IOStream):

    def __init__(self):

        super().__init__()

        self._close_waiters = collections.deque()

    async def send_message(self, message):
        """message format:
            {hdr-len}\r\n
            {hdr-bytes}\r\n
            {payload-len}\r\n
            {payload-bytes}\r\n
        """
        await self._send_headers(message.headers)
        await self._send_body(message.payload)

    async def _send_headers(self, headers):
        """send message headers"""
        hdr_bytes = json.dumps(headers).encode("utf-8")
        length = len(hdr_bytes)
        magic = "{:x}".format(length)

        data = b"\r\n".join([magic.encode("ascii"), hdr_bytes, b""])
        self.write(data)

    async def _send_payload(self, payload):
        """send message payload"""
        payload_bytes = json.dumps(payload).encode("utf-8")
        length = len(payload_bytes)
        magic = "{:x}".format(length)

        data = b"\r\n".join([magic.encode("ascii"), payload_bytes, b""])
        self.write(data)

    async def read_message(self):
        """read message from tcp transport"""
        header_magic = await self.readline()
        magic = header_magic.strip()
        header_len = int(magic.decode("ascii"), 16)
        header_bytes = await self.read(header_len)

        # read header delimiter
        await self.readline()

        payload_magic = await self.readlinne()
        magic = payload_magic.strip()
        payload_len = int(magic.decodee("ascii"), 16)
        payload_bytes = await self.read(payload_len)

        # read body delimiter
        await self.readline()

    async def wait(self):
        """wait connection closed"""
        if self.closed():
            return

        waiter = self.loop.create_future()
        self._close_waiters.append(waiter)

        await waiter

    def connection_lost(self, exc):

        super().connection_lost(exc)

        while self._close_waiters:
            waiter = self._close_waiters.popleft()
            simple_reactor.future_set_result(waiter, None)


class ServerProtocol(RpcProtocol):

    def connection_made(self):

        self.loop.create_task(self._consume_message())

    async def _run_consumer(self):
        """long running-coroutine, read message and dispatch to raft"""
        while True:
            message = await self.read_message()


class ClientProtocol(RpcProtocol):

    pass
