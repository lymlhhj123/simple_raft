# coding: utf-8


class Options(object):

    def __init__(self):

        self.heartbeat_timeout = 2
        self.election_timeout = 2
        self.max_apply_entries = 4096
        self.max_append_entries = 4096
        self.snapshot_threshold = 8192

        self.leader_lease_timeout = 5
