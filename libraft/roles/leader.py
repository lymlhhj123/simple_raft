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
        current_term = self.raft.current_term

        if ack_term > current_term:
            self.raft.stop_heartbeat()
            self.raft.vote_for = -1
            self.raft.to_follower()
            return

        if ack_term < current_term:
            return

        if rpc_data["success"] is False:
            self._follower_reply_failed(rpc.mem_id)
        else:
            self._follower_reply_success(rpc.mem_id)

        self._update_commit_index()
        self.raft.request_append_entries()

    def _follower_reply_failed(self, follower_id):
        """

        :param follower_id:
        :return:
        """
        self.raft.dec_next_index(follower_id)

    def _follower_reply_success(self, follower_id):
        """

        :param follower_id:
        :return:
        """
        log_index = self.raft.last_log_index()
        self.raft.update_next_index(follower_id, log_index)
        self.raft.update_match_index(follower_id, log_index)

    def _update_commit_index(self):
        """

        :return:
        """
        match_index = self.raft.match_index()
        index_map = {}
        for _, index in match_index.items():
            index_map.setdefault(index, 0)
            index_map[index] += 1

        node_count = len(self.raft.members())
        pre_match_index = 0
        for index, count in index_map.items():
            if count >= (node_count / 2 + 1):
                pre_match_index = index
                break

        if pre_match_index == 0:
            return

        log_entry = self.raft.get_log_entry(pre_match_index)
        assert log_entry

        if log_entry.term != self.raft.current_term:
            return

        if pre_match_index <= self.raft.commit_index:
            return

        self.raft.commit_index = pre_match_index
        self.raft.apply_log_entries()

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
