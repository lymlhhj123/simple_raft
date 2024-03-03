# coding: utf-8


class FSMStore(object):

    def take_snapshot(self):
        """create fsm snapshot"""

    def restore_snapshot(self, snapshot):
        """restore fsm by snapshot"""

    def last_apply_info(self):
        """return fsm last applied log index and term"""

    def apply(self, log_entries):
        """apply log entries to fsm"""


class FSMSnapshot(object):

    def __init__(self):

        self.name = ""
        self.snapshot_id = ""
        self.term = -1
        self.index = -1
        self.snapshot = ""
