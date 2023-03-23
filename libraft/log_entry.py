# coding: utf-8


class LogEntry(object):

    def __init__(self, index, term, data):

        self.index = index
        self.term = term
        self.data = data
