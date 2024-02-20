# coding: utf-8

from ..interfaces.snapshot_store import SnapShotStore


class SqliteSnapShotStore(SnapShotStore):

    async def list_snapshot(self):
        """list all snapshot"""

    async def open_snapshot(self, snapshot_id):
        """open snapshot by id"""

    async def create_snapshot(self):
        """create snapshot"""

    async def restore_snapshot(self):
        """restore snapshot"""
