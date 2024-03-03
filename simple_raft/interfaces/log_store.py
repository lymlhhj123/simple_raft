# coding: utf-8


class LogStore(object):

    async def first(self):
        """get first log entry"""

    async def last(self):
        """get last log entry"""

    async def find_one(self, log_index):
        """get log entry where index == log_index"""

    async def find_many(self, start, end):
        """get log entry where log_index between [start, end]"""

    async def insert_one(self, log_entry):
        """put single log entry to disks"""
        await self.insert_many([log_entry])

    async def insert_many(self, log_entries):
        """put multi log entries to disks"""

    async def delete_range(self, start, end):
        """delete log where log_index between [start, end]"""
