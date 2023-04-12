# coding: utf-8

from .role import Role
from .. import message


class Follower(Role):

    def __init__(self, raft):

        super(Follower, self).__init__(self.FOLLOWER, raft)

    def boostrap(self):
        """

        :return:
        """
        self.raft.reset_heartbeat_timeout()

    def process_vote_ack(self, rpc):
        """

        :param rpc:
        :return:
        """
        ack_msg = rpc.message
        ack_term = ack_msg["term"]
        current_term = self.raft.current_term
        if ack_term > current_term:
            self.raft.term = ack_term
            self.raft.vote_for = -1
            self.raft.reset_heartbeat_timeout()

    def process_vote_rpc(self, rpc):
        """

        :param rpc:
        :return:
        """
        data = self._process_vote_rpc(rpc)
        vote_ack = message.RequestVoteAckMessage(data, self.raft.my_id())
        self.raft.send_rpc(rpc.mem_id, vote_ack)

    def _process_vote_rpc(self, rpc):
        """

        :param rpc:
        :return:
        """
        vote_msg = rpc.message
        vote_term = vote_msg["term"]
        candidate_id = vote_msg["candidateId"]
        current_term = self.raft.current_term
        if vote_term < current_term:
            return {"term": current_term, "voteGranted": False}

        if vote_term == current_term:
            if self.raft.vote_for != -1 and self.raft.vote_for != candidate_id:
                return {"term": vote_term, "voteGranted": False}

        if vote_term > current_term:
            self.raft.current_term = vote_term

        if self.raft.is_log_newer_than_us(vote_msg["lastLogTerm"], vote_msg["lastLogIndex"]):
            vote_granted = True
        else:
            vote_granted = False

        self.raft.vote_for = candidate_id if vote_granted else -1
        self.raft.reset_heartbeat_timeout()
        return {"term": vote_term, "voteGranted": vote_granted}

    def process_append_entries_ack(self, rpc):
        """

        :param rpc:
        :return:
        """
        ack_msg = rpc.message
        ack_term = ack_msg["term"]
        if ack_term > self.raft.current_term:
            self.raft.current_term = ack_term
            self.raft.vote_for = -1
            self.raft.reset_heartbeat_timeout()

    def process_append_entries_rpc(self, rpc):
        """

        :param rpc:
        :return:
        """
        result = self._process_append_entries_rpc(rpc)
        rpc_ack = message.AppendEntriesAckMessage(result, self.raft.my_id())
        self.raft.send_rpc(rpc.mem_id, rpc_ack)

    def _process_append_entries_rpc(self, rpc):
        """

        :param rpc:
        :return:
        """
        rpc_msg = rpc.message
        current_term = self.raft.current_term
        leader_term = rpc_msg["term"]
        if leader_term < current_term:
            return {"term": current_term, "success": False}

        leader_id = rpc_msg["leaderId"]

        # what happened?
        if leader_term == current_term and self.raft.vote_for != leader_id:
            return {"term": current_term, "success": False}

        if leader_term > current_term:
            self.raft.current_term = leader_term
            self.raft.vote_for = leader_id

        prev_log_index = rpc_msg["prevLogIndex"]
        prev_log_term = rpc_msg["prevLogTerm"]
        if not self.raft.do_i_have_log_entry(prev_log_index, prev_log_term):
            succeed = False
        else:
            succeed = True
            self.raft.append_log_entries(rpc_msg["entries"], rpc_msg["leaderCommit"])
            self.raft.apply_log_entries()

        self.raft.reset_heartbeat_timeout()
        return {"term": leader_term, "success": succeed}
