# coding: utf-8

import json


class LogEntry(object):

    def __init__(self, index, term, data):

        self.index = index
        self.term = term
        self.data = data

    def dump(self):
        """

        :return:
        """
        return json.dumps({
            "index": self.index,
            "term": self.term,
            "data": self.data,
        })

    @classmethod
    def load(cls, obj):
        """

        :param obj:
        :return:
        """
        return cls(**json.loads(obj))
