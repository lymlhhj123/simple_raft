# coding: utf-8

from .role import Role
from .. import models


class Leader(Role):

    def __init__(self, raft):

        super(Leader, self).__init__(self.LEADER, raft)

    def boostrap(self):
        """

        :return:
        """
        self.raft.init_match_index()
        self.raft.init_next_index()
        self.raft.send_heartbeat()
        self.raft.start_heartbeat()

    def process_vote_ack(self, rpc):
        """handle vote ack from other members

        :param rpc:
        :return:
        """
        vote_ack = rpc.data
        ack_term = vote_ack["term"]
        current_term = self.raft.current_term
        if ack_term > current_term:
            self.raft.current_term = ack_term
            self.raft.stop_heartbeat()
            self.raft.to_follower()
            return

        if ack_term < current_term:
            return

        if vote_ack["voteGranted"] is False:
            return

        self.raft.add_quorum(rpc.mem_id)

    def process_vote_rpc(self, rpc):
        """handle request vote rpc from candidate

        if candidate's term <= our term, reply false;
        if candidate's term > our term, reply true;
        :param rpc:
        :return:
        """
        data = self._process_vote_rpc(rpc)
        vote_ack = models.VoteAckRpc(data, self.raft.my_id())
        self.raft.send_rpc(rpc.mem_id, vote_ack)

        if data["voteGranted"] is True:
            self.raft.stop_heartbeat()
            self.raft.to_follower()

    def _process_vote_rpc(self, rpc):
        """

        :param rpc:
        :return:
        """
        vote_msg = rpc.data
        current_term = self.raft.current_term
        if vote_msg["term"] <= current_term:
            return {"term": current_term, "voteGranted": False}

        self.raft.term = vote_msg["term"]
        self.raft.vote_who = vote_msg["candidateId"]
        return {"term": vote_msg["term"], "voteGranted": True}

    def process_append_entries_ack(self, rpc):
        """

        :param rpc:
        :return:
        """
        rpc_data = rpc.data
        ack_term = rpc_data["term"]

        if ack_term > self.raft.current_term:
            self.raft.stop_heartbeat()
            self.raft.vote_for = -1
            self.raft.to_follower()
            return

        if rpc_data["success"] is False:
            # dec nextIndex
            pass
        else:
            # inc nextIndex
            pass

        self._maybe_append_entries_alone()

    def _maybe_append_entries_alone(self):
        """maybe follower's log is not same as us

        :return:
        """

    def process_append_entries_rpc(self, rpc):
        """

        :param rpc:
        :return:
        """
        rpc_data = rpc.data
        leader_term = rpc_data["term"]

        if leader_term > self.raft.current_term:
            self.raft.stop_heartbeat()
            self.raft.vote_for = -1
            self.raft.to_follower()
            self.raft.dispatch_rpc(rpc)
