# coding: utf-8


class Role(object):

    LEADER = 0
    FOLLOWER = 1
    CANDIDATE = 2

    IDENTITY_MAP = {
        LEADER: "Leader",
        FOLLOWER: "Follower",
        CANDIDATE: "Candidate",
    }

    def __init__(self, identity, raft):

        self.identity = identity
        self.raft = raft

    def name(self):
        """

        :return:
        """
        return self.IDENTITY_MAP.get(self.identity, "Unknown")

    def is_leader(self):
        """

        :return:
        """
        return self.identity == self.LEADER

    def is_follower(self):
        """

        :return:
        """
        return self.identity == self.FOLLOWER

    def is_candidate(self):
        """

        :return:
        """
        return self.identity == self.CANDIDATE

    def boostrap(self):
        """

        :return:
        """

    def process_vote_ack(self, rpc):
        """handle vote ack from followers

        :param rpc:
        :return:
        """
        raise NotImplementedError

    def process_vote_rpc(self, rpc):
        """handle vote rpc from candidate

        :param rpc:
        :return:
        """
        raise NotImplementedError

    def process_append_entries_ack(self, rpc):
        """handle append log ack from followers

        :param rpc:
        :return:
        """
        raise NotImplementedError

    def process_append_entries_rpc(self, rpc):
        """handle append log rpc from leader

        :param rpc:
        :return:
        """
        raise NotImplementedError
