# coding: utf-8

import os
import json

from ..interfaces import fsm_store

import aiosqlite


class SqliteFSMStore(fsm_store.FSMStore):
    """
    fsm table:
    index  key  value
    0      k1   v1
    1      k2   v2

    meta table:
    index  key               value
    0      last_apply        {"log_term": 10, "log_index": 100}

    meta table only has one entry
    """

    def __init__(self, db_dir):

        self.db_path = os.path.join(db_dir, "fsm.db")
        self.fsm_table = "fsm"
        self.meta_table = "meta"

        self.db = None

    async def _connect(self):
        """sqlite dbpath connect"""
        if not self.db:
            self.db = await aiosqlite.connect(self.db_path)

            fsm_table = f"""
                CREATE TABLE {self.fsm_table} (
                    index_id INT PRIMARY KEY,
                    key TEXT NOT NULL UNIQUE, 
                    value TEXT, 
                );
            """

            self.db.execute(fsm_table)

            meta_table = f"""
                CREATE TABLE {self.meta_table} (
                    index_id INT PRIMARY KEY,
                    key TEXT NOT NULL UNIQUE, 
                    value TEXT, 
                );
            """

            self.db.execute(meta_table)

        return self.db

    async def take_snapshot(self):
        """create fsm snapshot"""
        db = await self._connect()

        async for row in db.iterdump():
            pass

    async def restore_snapshot(self, fsm_snapshot):
        """restore fsm by snapshot"""
        db = await self._connect()

        sql = f""
        await db.execute()

    async def last_apply_info(self):
        """return fsm last applied log index and term"""
        db = await self._connect()

        sql = f"SELECT value FROM {self.meta_table}"
        async with db.execute(sql) as cursor:
            row = await cursor.fetchone()
            if not row:
                return 0, 0

            return row["value"]

    async def apply(self, log_entries):
        """apply log entries to fsm"""
        db = await self._connect()

        sql = f"INSERT OR REPLACE INTO {self.fsm_table} (key, value) VALUES (?, ?)"

        # todo

        last_log = log_entries[-1]
        data = {"log_index": last_log.log_index, "log_term": last_log.log_term}
        sql = f"INSERT OR REPLACE INTO {self.meta_table} (key, value) VALUES (?, ?)"
        await db.execute(sql, ("last_apply", json.dumps(data)))
