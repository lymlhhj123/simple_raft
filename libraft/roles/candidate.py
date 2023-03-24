# coding: utf-8


from .role import Role
from .. import models


class Candidate(Role):
    
    def __init__(self, raft):
        
        super(Candidate, self).__init__(self.CANDIDATE, raft)

    def boostrap(self):
        """

        :return:
        """
        self.raft.clear_quorum()
        self.raft.start_election()
        self.raft.set_election_timeout()

    def process_vote_ack(self, rpc):
        """

        :param rpc:
        :return:
        """
        vote_msg = rpc.data
        ack_term = vote_msg["term"]
        current_term = self.raft.current_term
        if ack_term < current_term:
            return

        if ack_term > current_term:
            self.raft.cancel_election_timeout()
            self.raft.current_term = ack_term
            self.raft.vote_for = -1
            self.raft.to_follower()
            return

        if vote_msg["voteGranted"] is False:
            return

        self.raft.add_quorum(rpc.mem_id)

        if self.raft.do_i_election_win():
            self.raft.cancel_election_timeout()
            self.raft.election_victory()

    def process_vote_rpc(self, rpc):
        """

        :param rpc:
        :return:
        """
        data = self._process_vote_rpc(rpc)
        ack_rpc = models.VoteAckRpc(data, self.raft.my_id)
        self.raft.send_rpc(rpc.mem_id, ack_rpc)

    def _process_vote_rpc(self, rpc):
        """

        :param rpc:
        :return:
        """
        vote_msg = rpc.data
        vote_term = vote_msg["term"]
        candidate_id = vote_msg["candidateId"]
        current_term = self.raft.current_term
        if vote_term <= current_term:
            return {"term": current_term, "voteGranted": False}

        self.raft.current_term = vote_term

        if self.raft.is_log_newer_than_our(vote_msg["lastLogTerm"], vote_msg["lastLogIndex"]):
            vote_granted = True
        else:
            vote_granted = False

        self.raft.cancel_election_timeout()
        self.raft.vote_for = candidate_id if vote_granted else -1
        self.raft.to_follower()
        return {"term": vote_term, "voteGranted": vote_granted}

    def process_append_entries_ack(self, rpc):
        """

        :param rpc:
        :return:
        """
        rpc_data = rpc.data
        ack_term = rpc_data["term"]

        if ack_term > self.raft.current_term:
            self.raft.current_term = ack_term
            self.raft.vote_for = -1
            self.raft.cancel_election_timeout()
            self.raft.to_follower()

    def process_append_entries_rpc(self, rpc):
        """

        :param rpc:
        :return:
        """
        rpc_data = rpc.data
        leader_term = rpc_data["term"]

        if leader_term >= self.raft.current_term:
            self.raft.cancel_election_timeout()
            self.raft.vote_for = -1
            self.raft.to_follower()
            self.raft.dispatch_rpc(rpc)
