# coding: utf-8

import os

from ..interfaces.log_store import LogStore
from .. import log

import aiosqlite


class SqliteLogStore(LogStore):
    """
    db table:

    index_id  log_index  log_term  log_type   data
    1         1          1         1          log_1
    2         2          1         0          log_2

    index is auto increment
    """

    def __init__(self, db_dir):

        self.db_path = os.path.join(db_dir, "log.db")
        self.table = "log_store"
        self.db = None

    async def _connect(self):
        """sqlite dbpath connect"""
        if not self.db:
            self.db = await aiosqlite.connect(self.db_path)
            self.db.row_factory = aiosqlite.Row

            table_define = f"""
                CREATE TABLE {self.table} (
                    index_id INT PRIMARY KEY,
                    log_index INT NOT NULL INDEX,
                    log_term INT NOT NULL,
                    log_type INT NOT NULL,
                    data TEXT,
                );
            """

            await self.db.execute(table_define)

        return self.db

    async def first(self):
        """get first log entry"""
        db = await self._connect()

        sql = f"SELECT * FROM {self.table} ORDER BY index_id ASC LIMIT 1"
        async with db.execute(sql) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None

            return load_log_entry(row)

    async def last(self):
        """get last log entry"""
        db = await self._connect()

        sql = f"SELECT * FROM {self.table} ORDER BY index_id DESC LIMIT 1"
        async with db.execute(sql) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None

            return load_log_entry(row)

    async def find_one(self, log_index):
        """get log entry by log_index"""
        db = await self._connect()

        sql = f"SELECT * FROM {self.table} WHERE log_index = ?"
        async with db.execute(sql, (log_index, )) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None

            return load_log_entry(row)

    async def find_many(self, start, end):
        """get log entry where log_index between [start, end]"""
        db = await self._connect()

        sql = (f"SELECT * FROM {self.table} "
               f"WHERE log_index >= ? AND log_index <= ? "
               f"ORDER BY log_index ASC")

        logs = []
        async with db.execute(sql, (start, end)) as cursor:
            async for row in cursor:
                logs.append(load_log_entry(row))

        return logs

    async def insert_one(self, log_entry):
        """put single log entry to disks"""
        await self.insert_many([log_entry])

    async def insert_many(self, log_entries):
        """put multi log entries to disks"""
        db = await self._connect()

        values = []
        for entry in log_entries:
            values.append(entry.as_tuple())

        sql = f"INSERT INTO {self.table} (log_index, log_term, log_type, data) VALUES (?, ?, ?, ?)"
        await db.executemany(sql, values)
        await db.commit()

    async def delete_range(self, start, end):
        """delete log where log_index between [start, end]"""
        db = await self._connect()

        sql = f"DELETE FROM {self.table} WHERE log_index >= ? AND log_index <= ?"
        await db.execute(sql, (start, end))
        await db.commit()


def load_log_entry(sqlite_row):
    """load log entry"""
    log_entry = log.LogEntry()
    log_entry.log_index = sqlite_row["log_index"]
    log_entry.log_term = sqlite_row["log_term"]
    log_entry.log_type = sqlite_row["log_type"]
    log_entry.data = sqlite_row["data"]
    return log_entry
