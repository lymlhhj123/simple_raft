# coding: utf-8

import json

REQUEST_VOTE = 1
REQUEST_VOTE_RESP = 2
APPEND_ENTRIES = 3
APPEND_ENTRIES_RESP = 4
INSTALL_SNAPSHOT = 5
INSTALL_SNAPSHOT_RESP = 6


class Message(object):

    def __init__(self, headers, data):

        self.headers = headers
        self.payload = data

    def __repr__(self):

        return f"Message<op: {self.headers["op"]}>"

    @classmethod
    def create_request_vote_req(cls, headers, data):

        headers["op"] = REQUEST_VOTE
        return cls(headers, data)

    @classmethod
    def create_request_vote_resp(cls, headers, data):

        headers["op"] = REQUEST_VOTE_RESP
        return cls(headers, data)

    @classmethod
    def create_append_entries_req(cls, headers, data):

        headers["op"] = APPEND_ENTRIES
        return cls(headers, data)

    @classmethod
    def create_append_entries_resp(cls, headers, data):

        headers["op"] = APPEND_ENTRIES_RESP
        return cls(headers, data)

    @classmethod
    def create_install_snapshot_req(cls, headers, data):

        headers["op"] = INSTALL_SNAPSHOT
        return cls(headers, data)

    @classmethod
    def create_install_snapshot_resp(cls, headers, data):

        headers["op"] = INSTALL_SNAPSHOT_RESP
        return cls(headers, data)

    def is_request_vote_req(self):
        """return True"""
        return self.op() == REQUEST_VOTE

    def is_request_vote_resp(self):
        """return True"""
        return self.op() == REQUEST_VOTE_RESP

    def is_append_entries_req(self):
        """return True"""
        return self.op() == APPEND_ENTRIES

    def is_append_entries_resp(self):
        """return True"""
        return self.op() == APPEND_ENTRIES_RESP

    def is_install_snapshot_req(self):

        return self.op() == INSTALL_SNAPSHOT

    def is_install_snapshot_resp(self):

        return self.op() == INSTALL_SNAPSHOT_RESP

    def op(self):
        """return message op"""
        return self.headers["op"]

    def addr(self):
        """return message addr"""
        return self.headers["server_addr"]
