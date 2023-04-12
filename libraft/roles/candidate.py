# coding: utf-8


from .role import Role
from .. import message


class Candidate(Role):
    
    def __init__(self, raft):
        
        super(Candidate, self).__init__(self.CANDIDATE, raft)

    def boostrap(self):
        """

        :return:
        """
        self.raft.clear_quorum()
        self.raft.start_election()

    def process_vote_ack(self, rpc):
        """

        :param rpc:
        :return:
        """
        vote_msg = rpc.message
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

        self.raft.add_quorum(vote_msg.node_id)

        if self.raft.do_i_election_win():
            self.raft.cancel_election_timeout()
            self.raft.election_victory()

    def process_vote_rpc(self, rpc):
        """

        :param rpc:
        :return:
        """
        data = self._process_vote_rpc(rpc)
        ack_rpc = message.RequestVoteAckMessage(data, self.raft.my_id)
        self.raft.send_rpc(rpc.mem_id, ack_rpc)

    def _process_vote_rpc(self, rpc):
        """

        :param rpc:
        :return:
        """
        vote_msg = rpc.message
        vote_term = vote_msg["term"]
        current_term = self.raft.current_term
        if vote_term <= current_term:
            return {"term": current_term, "voteGranted": False}

        self.raft.current_term = vote_term

        if self.raft.is_log_newer_than_us(vote_msg["lastLogTerm"], vote_msg["lastLogIndex"]):
            vote_granted = True
        else:
            vote_granted = False

        self.raft.cancel_election_timeout()
        self.raft.vote_for = vote_msg["candidateId"] if vote_granted else -1
        self.raft.to_follower()
        return {"term": vote_term, "voteGranted": vote_granted}

    def process_append_entries_ack(self, rpc):
        """

        :param rpc:
        :return:
        """
        ack_msg = rpc.message
        ack_term = ack_msg["term"]

        if ack_term > self.raft.current_term:
            self.raft.cancel_election_timeout()
            self.raft.current_term = ack_term
            self.raft.vote_for = -1
            self.raft.to_follower()

    def process_append_entries_rpc(self, rpc):
        """

        :param rpc:
        :return:
        """
        append_entries_msg = rpc.message
        leader_term = append_entries_msg["term"]

        current_term = self.raft.current_term
        if leader_term < current_term:
            pass
        else:
            self.raft.cancel_election_timeout()
            self.raft.current_term = leader_term
            self.raft.vote_for = append_entries_msg["leaderId"]
            self.raft.to_follower()
            self.raft.dispatch_rpc(rpc)
