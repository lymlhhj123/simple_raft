# coding: utf-8

OP_APPLY_SINGLE = 0
OP_APPLY_BATCH = 1
OP_TAKE_SNAPSHOT = 2
OP_RESTORE_SNAPSHOT = 3


class FSM(object):

    def __init__(self, raft):

        self.raft = raft
        self.loop = self.raft.get_loop()

        self._queue = self.loop.create_fifo_queue()

    async def apply_single(self, log_entry):
        """apply single log entry"""
        waiter = self.loop.create_future()
        await self._queue.put((waiter, OP_APPLY_SINGLE, log_entry))
        await waiter

    async def apply_batch(self, log_entries):
        """apply batch log entries"""
        waiter = self.loop.create_future()
        await self._queue.put((waiter, OP_APPLY_BATCH, log_entries))
        await waiter

    async def take_snapshot(self):
        """create fsm snapshot"""
        waiter = self.loop.create_future()
        await self._queue.put((waiter, OP_TAKE_SNAPSHOT, None))
        await waiter

    async def restore_snapshot(self, snapshot):
        """restore fsm snapshot"""


class FSMSnapshot(object):

    pass
