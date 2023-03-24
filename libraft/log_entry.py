# coding: utf-8

import json


class LogEntry(object):

    def __init__(self, index, term, data):

        self.index = index
        self.term = term
        self.data = data

    @classmethod
    def from_json(cls, json_obj):
        """

        :param json_obj:
        :return:
        """
        return cls(**json.loads(json_obj))

    def to_json(self):
        """

        :return:
        """
        v = {
            "index": self.index,
            "term": self.term,
            "data": self.data,
        }
        return json.dumps(v)
