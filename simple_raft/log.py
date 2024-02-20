# coding: utf-8

LOG_COMMAND = 0
LOG_NOOP = 1
LOG_UNKNOWN = 2


class LogEntry(object):

    def __init__(self):

        self.type = LOG_UNKNOWN
        self.index = 0
        self.term = 0
        self.data = b""

    @classmethod
    def from_json(cls, data):
        """construct from python dict"""
        log = cls()
        log.type = data["type"]
        log.index = data["index"]
        log.term = data["term"]
        log.data = data["data"]
        return log

    def as_json(self):
        """return as python dict"""
        data = {
            "type": self.type,
            "index": self.index,
            "term": self.term,
            "data": self.data
        }
        return data


class FutureEntry(object):

    def __init__(self, log_entry, future):

        self.log_entry = log_entry
        self.future = future


class LogCreator(object):

    def __init__(self, raft):

        self.raft = raft
        self.loop = raft.get_loop()

    def create_noop_log(self):
        """create noop log entry"""
        return self.create_log(LOG_NOOP, "")

    def create_command_log(self, command):
        """create command log entry"""
        return self.create_log(LOG_COMMAND, command)

    def create_log(self, log_type, command):
        """new log entry, we don't set index and term in here"""
        log_entry = LogEntry()
        log_entry.type = log_type
        log_entry.command = command
        return log_entry

    def wrap_as_future(self, log_entry):
        """wrap log entry to future entry"""
        future = self.loop.create_future()
        return FutureEntry(log_entry, future)
