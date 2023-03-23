# coding: utf-8

from .role import Role
from .. import models


class Follower(Role):

    def __init__(self, raft):

        super(Follower, self).__init__(self.FOLLOWER, raft)

    def boostrap(self):
        """

        :return:
        """
        self.raft.reset_heartbeat_timeout()

    def process_vote_ack(self, rpc):
        """处理请求投票ack的rpc

        我们的身份是follower，可能我们之前发起选举，之后接受到了一个更大的term，身份由候选人变成了跟随者。
        :param rpc:
        :return:
        """
        vote_ack = rpc.data
        ack_term = vote_ack["term"]
        current_term = self.raft.current_term
        if ack_term > current_term:
            self.raft.term = ack_term
            self.raft.vote_for = -1
            self.raft.reset_heartbeat_timeout()

    def process_vote_rpc(self, rpc):
        """处理来自候选人请求投票的rpc

        :param rpc:
        :return:
        """
        data = self._process_vote_rpc(rpc)
        vote_ack = models.VoteAckRpc(data, self.raft.my_id())
        self.raft.send_rpc(rpc.mem_id, vote_ack)

        if data["voteGranted"] is True:
            self.raft.reset_heartbeat_timeout()

    def _process_vote_rpc(self, rpc):
        """

        :param rpc:
        :return:
        """
        vote_syn = rpc.data
        vote_term = vote_syn["term"]
        candidate_id = vote_syn["candidateId"]
        current_term = self.raft.current_term
        if vote_term < current_term:
            return {"term": current_term, "voteGranted": False}

        if vote_term > current_term:
            self.raft.vote_for = candidate_id
            self.raft.current_term = vote_term
            return {"term": vote_term, "voteGranted": True}

        # 一个任期号只能投票一次
        if self.raft.vote_for != -1 or self.raft.vote_for != candidate_id:
            return {"term": vote_term, "voteGranted": False}

        last_log_term = vote_syn["lastLogTerm"]
        last_log_index = vote_syn["lastLogIndex"]

        # 最后一条日志的任期比自己旧，拒绝投票
        if last_log_term < self.raft.last_log_term():
            return {"term": current_term, "voteGranted": False}

        # 最后一条日志的任期比自己大，或者最后一条日志的索引不比自己小，同意投票
        # 如果最后一条日志的任期跟自己相等，但是索引比自己的小，不能投票
        if last_log_term > self.raft.last_log_term() or last_log_index >= self.raft.last_log_index():
            self.raft.vote_for = candidate_id
            return {"term": vote_term, "voteGranted": True}

        self.raft.vote_for = -1
        return {"term": vote_term, "voteGranted": False}

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
            self.raft.reset_heartbeat_timeout()

    def process_append_entries_rpc(self, rpc):
        """

        :param rpc:
        :return:
        """
        result = self._process_append_entries_rpc(rpc)
        if not result:
            return

        rpc_ack = models.AppendEntriesAckRpc(result, self.raft.my_id())
        self.raft.send_rpc(rpc.mem_id, rpc_ack)

    def _process_append_entries_rpc(self, rpc):
        """

        :param rpc:
        :return:
        """
        rpc_data = rpc.data
        current_term = self.raft.current_term
        leader_term = rpc_data["term"]
        leader_id = rpc_data["leaderId"]
        if leader_term < current_term:
            return {"term": leader_term, "success": False}

        # todo: maybe something wrong
        if leader_term == current_term:
            assert self.raft.vote_for == leader_id

        if leader_term > current_term:
            self.raft.current_term = leader_term
            self.raft.vote_for = leader_id

        prev_log_index = rpc_data["prevLogIndex"]
        prev_log_term = rpc_data["prevLogTerm"]
        if not self._check_log_entry(prev_log_index, prev_log_term):
            succeed = False
        else:
            self._append_entries(rpc_data["entries"], rpc_data["leaderCommit"])
            self.raft.apply_log_entries()
            succeed = True

        self.raft.reset_heartbeat_timeout()
        return {"term": leader_term, "success": succeed}

    def _check_log_entry(self, log_index, log_term):
        """

        :param log_index:
        :return:
        """
        log_entry = self.raft.get_log(log_index)
        if not log_entry or log_entry.term != log_term:
            return False

        return True

    def _append_entries(self, log_entries, leader_commit):
        """

        :param log_entries:
        :param leader_commit:
        :return:
        """
        log_offset = 0
        for offset, entry in enumerate(log_entries):
            log_offset = offset
            my_entry = self.raft.get_log_entry(entry.index)
            if not my_entry:
                break

            if my_entry.term != entry.term:
                self.raft.delete_log_entries(my_entry.index)
                break

        log_entries = log_entries[log_offset:]
        if log_entries:
            self.raft.save_log_entries(log_entries)
            self.raft.commit_index = min(leader_commit, log_entries[-1].index)
