# coding: utf-8

from ..interfaces.fsm import FSM


class SqliteFSM(FSM):

    def take_snapshot(self):
        """create fsm snapshot"""

    def restore_snapshot(self, snapshot):
        """restore fsm by snapshot"""

    def last(self):
        """return fsm last applied log index and term"""

    def apply(self, log_entries):
        """apply log entries to fsm"""
