# coding: utf-8

LOG_UNKNOWN = -1
LOG_COMMAND = 0
LOG_NOOP = 1


class LogEntry(object):

    def __init__(self):

        self.log_type = LOG_UNKNOWN
        self.log_index = -1
        self.log_term = -1
        self.data = ""

    def as_json(self):
        """return as dict"""
        data = {
            "log_type": self.log_type,
            "log_index": self.log_index,
            "log_term": self.log_term,
            "data": self.data
        }
        return data

    def as_tuple(self):
        """return as tuple"""
        return self.log_index, self.log_term, self.log_type, self.data


class FutureEntry(object):

    def __init__(self, log_entry, future):

        self.log_entry = log_entry
        self.future = future


def new_noop_log():
    """create no-op log entry"""
    log_entry = LogEntry()
    log_entry.type = LOG_NOOP
    log_entry.data = ""
    return log_entry


def new_command_log(data):
    """create command log entry"""
    log_entry = LogEntry()
    log_entry.type = LOG_COMMAND
    log_entry.data = data
    return log_entry


def wrap_future(log_entry, fut):
    """wrap log entry to future entry"""
    return FutureEntry(log_entry, fut)
