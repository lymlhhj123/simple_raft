# coding: utf-8

import os

from ..interfaces.stable_store import StableStore

import aiosqlite


class SqliteStableStore(StableStore):
    """
    stable_store table:

    index  key  value
    1      k1   v1
    2      k2   v2
    """

    def __init__(self, db_path):

        self.db_path = os.path.join(db_path, "stable.db")
        self.table = "stable_store"

        self.db = None

    async def _connect(self):
        """sqlite connect to dbpath"""
        if not self.db:
            self.db = await aiosqlite.connect(self.db_path)
            self.db.row_factory = aiosqlite.Row

            table_define = f"""
                CREATE TABLE {self.table} (
                    index_id INT PRIMARY KEY,
                    key TEXT NOT NULL UNIQUE, 
                    value TEXT, 
                );
            """

            await self.db.execute(table_define)

        return self.db

    async def put_int(self, k, v):
        """put int value to store"""
        assert isinstance(v, int)
        await self.put(k, str(v))

    async def put(self, k, v):
        """put k=v in the store, k and v must be str"""
        sql = f"INSERT OR REPLACE INTO {self.table} (key, value) VALUES (?, ?)"
        await self.db.execute(sql, (k, v))
        await self.db.commit()

    async def get_int(self, k, default=0):
        """get int value from store"""
        v = await self.get(k, default)
        return int(v)

    async def get(self, k, default=None):
        """get v where key=k, if not exist, return default"""
        sql = f"SELECT value FROM {self.table} WHERE key = ?"
        async with self.db.execute(sql, (k, )) as cursor:
            row = await cursor.fetchone()
            if not row:
                return default

            return row["value"]

    async def delete(self, k):
        """delete k in the store"""
        sql = f"DELETE FROM {self.table} WHERE key = ?"
        await self.db.execute(sql, (k, ))
        await self.db.commit()
