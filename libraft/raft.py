# coding: utf-8

import os
import random

from .storage import Storage
from . import roles
from . import message
from .log_entry import LogEntry

from libreactor import EventLoopThread

LEADER = 0
CANDIDATE = 1
FOLLOWER = 2


class Raft(object):

    def __init__(self, conf_file, rpc_service):

        self._conf_file = conf_file
        self._rpc_service = rpc_service

        self._current_term = -1
        self._vote_for = -1

        self._commit_index = -1
        self._last_applied = -1
        self._last_log_index = -1
        self._last_log_term = -1

        self._next_index = {}
        self._match_index = {}

        self._my_role = None
        self._my_id = -1
        self._my_name = os.uname()[1]

        # id -> ip
        self._node_map = {}

        # ["host1", "host2"]
        self._node_list = []

        # id list
        self._quorum_set = set()

        # id list
        self._log_synced = set()

        self._storage = Storage()

        ev_t = EventLoopThread()
        ev_t.start()

        self._ev = ev_t.get_event_loop()

        self._heartbeat_interval = 0.2

        self._election_timeout_timer = None

        self._heartbeat_timer = None
        self._heartbeat_timeout_timer = None

    def members(self):
        """

        :return:
        """
        return self._node_list

    def quorums(self):
        """

        :return:
        """
        return self._quorum_set

    def add_quorum(self, mem_id):
        """

        :param mem_id:
        :return:
        """
        self._quorum_set.add(mem_id)

    def clear_quorum(self):
        """

        :return:
        """
        self._quorum_set.clear()

    @property
    def my_id(self):
        """

        :return:
        """
        return self._my_id

    def storage(self):
        """

        :return:
        """
        return self._storage

    @property
    def last_log_index(self):
        """

        :return:
        """
        return self._last_log_index

    @last_log_index.setter
    def last_log_index(self, log_index):
        """

        :param log_index:
        :return:
        """
        self._last_log_index = log_index

    @property
    def last_log_term(self):
        """

        :return:
        """
        return self._last_log_term

    @last_log_term.setter
    def last_log_term(self, log_term):
        """

        :param log_term:
        :return:
        """
        self._last_log_term = log_term

    def init_match_index(self):
        """

        :return:
        """

    def match_index(self):
        """

        :return:
        """
        return self._match_index

    def update_match_index(self, mem_id, log_idx):
        """

        :param mem_id:
        :param log_idx:
        :return:
        """
        self._match_index[mem_id] = log_idx

    def clear_match_index(self):
        """

        :return:
        """
        self._match_index.clear()

    def init_next_index(self):
        """

        :return:
        """

    def dec_next_index(self, mem_id):
        """

        :param mem_id:
        :return:
        """
        self._next_index[mem_id] -= 1

    def update_next_index(self, mem_id, log_index):
        """

        :param mem_id:
        :param log_index:
        :return:
        """
        self._next_index[mem_id] = log_index

    def clear_next_index(self):
        """

        :return:
        """
        self._next_index.clear()

    def send_rpc(self, mem_id, rpc):
        """

        :param mem_id:
        :param rpc:
        :return:
        """
        dst = self._node_list[mem_id]
        self._rpc_service.send_rpc(dst, rpc)

    def to_leader(self):
        """

        :return:
        """
        self._my_role = roles.Leader(self)
        self._my_role.boostrap()

    def to_follower(self, term, leader_id):
        """

        :param term:
        :param leader_id:
        :return:
        """
        assert term > self.current_term

        my_role = self._my_role

        if my_role == LEADER:
            self.stop_heartbeat()
        elif my_role == CANDIDATE:
            self.cancel_election_timeout()
        else:
            self.cancel_heartbeat_timeout()

        self.vote_for = leader_id
        self._my_role = FOLLOWER

    def to_candidate(self):
        """

        :return:
        """
        self._my_role = roles.Candidate(self)
        self._my_role.boostrap()

    def boostrap(self):
        """

        :return:
        """
        # todo
        self._load_config()
        self._load_meta()

        self.to_follower()

    def _load_config(self):
        """

        :return:
        """
        self._node_list = []

        self._my_id = self._node_list.index(self._my_name)

        self._node_map = {}

    def _load_meta(self):
        """

        :return:
        """
        self._commit_index = self.storage().get_uint64("commitIndex", 0)
        self._last_applied = self.storage().get_uint64("lastApplied", 0)
        self._current_term = self.storage().get_uint64("currentTerm", 0)
        self._vote_for = self.storage().get_int64("voteFor", -1)

    @property
    def commit_index(self):
        """

        :return:
        """
        return self._commit_index

    @commit_index.setter
    def commit_index(self, log_index):
        """

        :param log_index:
        :return:
        """
        self._commit_index = log_index
        self._storage.put_uint64("commitIndex", log_index)

    @property
    def last_applied(self):
        """

        :return:
        """
        return self._last_applied

    @last_applied.setter
    def last_applied(self, log_index):
        """

        :param log_index:
        :return:
        """
        self._last_applied = log_index
        self._storage.put_uint64("lastApplied", log_index)

    @property
    def current_term(self):
        """

        :return:
        """
        return self._current_term

    @current_term.setter
    def current_term(self, new_term):
        """

        :param new_term:
        :return:
        """
        self._current_term = new_term
        self._storage.put_uint64("currentTerm", new_term)

    @property
    def vote_for(self):
        """

        :return:
        """
        return self._vote_for

    @vote_for.setter
    def vote_for(self, candidate_id):
        """

        :param candidate_id:
        :return:
        """
        self._vote_for = candidate_id

    def get_log_entry(self, log_index):
        """

        :param log_index:
        :return:
        """
        # todo
        entry_raw = self.storage().get(log_index)
        return LogEntry.from_json(entry_raw)

    def delete_log_entry(self, log_index):
        """

        :param log_index:
        :return:
        """

    def delete_log_entries(self, start_index):
        """

        :param start_index:
        :return:
        """
        for log_index in range(start_index, self.last_log_index + 1):
            self.storage().delete(log_index)

    def is_log_newer_than_us(self, last_log_term, last_log_index):
        """

        :param last_log_term:
        :param last_log_index:
        :return:
        """
        if last_log_term > self.last_log_term:
            return True

        if last_log_term == self.last_log_term:
            if last_log_index >= self.last_log_index:
                return True

        return False

    def do_i_have_log_entry(self, log_index, log_term):
        """

        :param log_index:
        :param log_term:
        :return:
        """
        log_entry = self.get_log_entry(log_index)
        if not log_entry or log_entry.term != log_term:
            return False

        return True

    def start_election(self):
        """

        :return:
        """
        assert self._my_role.is_candidate()

        self.current_term += 1

        if len(self.members()) == 1:
            self.election_victory_standalone()
        else:
            self.request_vote()

    def request_vote(self):
        """request members to vote us as leader

        :return:
        """
        self.vote_for = self.my_id
        self.add_quorum(self.my_id)

        vote_msg = {
            "term": self.current_term,
            "candidateId": self.my_id,
            "lastLogIndex": self.last_log_index,
            "lastLogTerm": self.last_log_term,
        }

        # todo: send vote rpc to all members

        self.set_election_timeout()

    def set_election_timeout(self):
        """set election timeout, timeout is between 150 and 300 msec

        :return:
        """
        assert self._my_role.is_candidate()
        timeout = 0.15 * (random.random() + 1)
        self._election_timeout_timer = self._ev.call_later(timeout, self._election_timeout)

    def _election_timeout(self):
        """

        :return:
        """
        self.to_candidate()

    def cancel_election_timeout(self):
        """candidate cancel election timeout

        :return:
        """
        assert self._my_role.is_candidate()
        if self._election_timeout_timer:
            self._election_timeout_timer.cancel()

    def do_i_election_win(self):
        """check if we win the election

        :return:
        """
        assert self._my_role.is_candidate()
        return len(self.quorums()) >= (len(self.members()) / 2 + 1)

    def election_victory_standalone(self):
        """

        :return:
        """
        assert self._my_role.is_candidate()
        self.add_quorum(self.my_id)

        self.election_victory()

    def election_victory(self):
        """candidate win election

        :return:
        """
        assert self._my_role.is_candidate()
        self.to_leader()

    def start_heartbeat(self):
        """leader start heartbeat after win election

        :return:
        """
        assert self._my_role.is_leader()
        self._heartbeat_timer = self._ev.call_every(self._heartbeat_interval, self.send_heartbeat)

    def send_heartbeat(self):
        """leader send heartbeat to members

        :return:
        """

    def stop_heartbeat(self):
        """leader stop heartbeat with members

        :return:
        """
        assert self._my_role.is_leader()
        if self._heartbeat_timer:
            self._heartbeat_timer.cancel()
            self._heartbeat_timer = None

    def reset_heartbeat_timeout(self):
        """follower reset heartbeat timeout

        :return:
        """
        self.cancel_heartbeat_timeout()
        self.set_heartbeat_timeout()

    def set_heartbeat_timeout(self):
        """follower set heartbeat timeout

        :return:
        """
        delay = self._heartbeat_interval * 2
        self._heartbeat_timeout_timer = self._ev.call_later(delay, self._heartbeat_timeout)

    def _heartbeat_timeout(self):
        """

        :return:
        """
        self.to_candidate()

    def cancel_heartbeat_timeout(self):
        """follower cancel heartbeat timeout

        :return:
        """
        if self._heartbeat_timeout_timer:
            self._heartbeat_timeout_timer.cancel()

    def request_append_entries(self):
        """

        :return:
        """
        assert self._my_role.is_leader()

        append_entries_syn = {
            "term": self.current_term,
            "leaderId": self.my_id,
            "prevLogIndex": -1,
            "prevLogTerm": -1,
            "entries": [],
            "leaderCommit": self.commit_index,
        }

    def append_log_entries(self, log_entries, leader_commit):
        """

        :param log_entries:
        :param leader_commit:
        :return:
        """
        log_offset = 0
        for offset, entry in enumerate(log_entries):
            log_offset = offset
            my_entry = self.get_log_entry(entry.index)
            if not my_entry:
                break

            if my_entry.term != entry.term:
                self.delete_log_entries(my_entry.index)
                break

        log_entries = log_entries[log_offset:]
        if not log_entries:
            return

        self.save_log_entries(log_entries)
        self.commit_index = min(leader_commit, log_entries[-1].index)
        self.last_log_index = log_entries[-1].index
        self.last_log_term = log_entries[-1].term

    def save_log_entries(self, log_entries):
        """

        :param log_entries:
        :return:
        """

    def apply_log_entries(self):
        """

        :return:
        """
        if self.last_applied >= self.commit_index:
            return

        start_index = self.last_applied + 1
        while start_index <= self.commit_index:
            log_entry = self.get_log_entry(start_index)
            self.apply_log_entry(log_entry)
            start_index += 1

        self.last_applied = self.commit_index

    def apply_log_entry(self, log_entry):
        """

        :param log_entry:
        :return:
        """

    def dispatch_rpc(self, rpc):
        """

        :param rpc:
        :return:
        """
        msg_op = rpc.message.op
        if msg_op == message.REQUEST_VOTE:
            self._my_role.process_vote_rpc(rpc)
        elif msg_op == message.REQUEST_VOTE_ACK:
            self._my_role.process_vote_ack(rpc)
        elif msg_op == message.APPEND_ENTRIES:
            self._my_role.process_append_entries_rpc(rpc)
        elif msg_op == message.APPEND_ENTRIES_ACK:
            self._my_role.process_append_entries_ack(rpc)
        else:
            pass

    def process_request_vote(self, rpc):
        """

        :param rpc:
        :return:
        """
        vote_msg = rpc.message
        vote_term = vote_msg["term"]
        candidate_id = vote_msg["candidateId"]
        last_log_term = vote_msg["lastLogTerm"]
        last_log_index = vote_msg["lastLogIndex"]

        data = self._process_request_vote(vote_term, candidate_id, last_log_term, last_log_index)
        ack_rpc = message.RequestVoteAckMessage(data, self.my_id)
        self.send_rpc(rpc.node_id, ack_rpc)

    def _process_request_vote(self, vote_term, candidate_id, last_log_term, last_log_index):
        """

        :param vote_term:
        :param candidate_id:
        :param last_log_term:
        :param last_log_index:
        :return:
        """
        current_term = self.current_term

        if vote_term < current_term:
            return {"term": current_term, "voteGranted": False}
        elif vote_term == current_term:
            if self._my_role in (LEADER, CANDIDATE):
                return {"term": current_term, "voteGranted": False}
            else:
                if self.vote_for != -1 and self.vote_for != candidate_id:
                    return {"term": current_term, "voteGranted": False}
        else:
            self.current_term = vote_term

            if self._my_role == CANDIDATE:
                self.cancel_election_timeout()
            elif self._my_role == LEADER:
                self.stop_heartbeat()
            else:
                self.cancel_heartbeat_timeout()

            self._my_role = FOLLOWER

        self.reset_heartbeat_timeout()

        if self.is_log_newer_than_us(last_log_term, last_log_index):
            vote_granted = True
        else:
            vote_granted = False

        self.vote_for = candidate_id if vote_granted else -1
        return {"term": vote_term, "voteGranted": vote_granted}

    def process_request_vote_ack(self, rpc):
        """

        :param rpc:
        :return:
        """

    def process_append_entries(self, rpc):
        """

        :param rpc:
        :return:
        """
        assert self._my_role != LEADER

        rpc_msg = rpc.message
        leader_term = rpc_msg["term"]
        leader_id = rpc_msg["leaderId"]

    def _process_append_entries(self, leader_term, leader_id, prev_log_index,
                                prev_log_term, log_entries, leader_commit):
        """

        :param leader_term:
        :param leader_id:
        :param prev_log_term:
        :param prev_log_index:
        :param log_entries:
        :return:
        """
        current_term = self.current_term
        if leader_term < current_term:
            return {"term": current_term, "success": False, "lastLogIndex": self.last_log_index}
        elif leader_term == current_term:
            assert self._my_role != LEADER
            if self._my_role == CANDIDATE:
                self._my_role = FOLLOWER

            self.vote_for = leader_id
        else:
            self.to_follower(leader_term, leader_id)

        if current_term != leader_term:
            self.current_term = leader_term

        if self.vote_for != leader_id:
            self.vote_for = leader_id

        self.reset_heartbeat_timeout()

        if not self.do_i_have_log_entry(prev_log_index, prev_log_term):
            succeed = False
        else:
            succeed = True
            self.append_log_entries(log_entries, leader_commit)
            self.apply_log_entries()

        return {"term": leader_term, "success": succeed, "lastLogIndex": self.last_log_index}

    def process_append_entries_ack(self, rpc):
        """

        :param rpc:
        :return:
        """

    def _process_append_entries_ack(self, rpc_msg):
        """

        :param rpc_msg:
        :return:
        """
        peer_term = rpc_msg["term"]
        current_term = self.current_term
        if peer_term < current_term:
            return

        if peer_term > current_term:
            self.to_follower(peer_term, -1)
            self.reset_heartbeat_timeout()
            return

        assert self._my_role == LEADER

        if rpc_msg["success"] is False:
            self._follower_reply_failed(rpc_msg)
            return

        self._follower_reply_success(rpc_msg)

        self._log_synced.add(rpc_msg.node_id)

        if len(self._log_synced) < len(self._quorum_set):
            return

        self._update_commit_index()
        self.raft.request_append_entries()

    def _follower_reply_failed(self, rpc_msg):
        """

        :param rpc_msg:
        :return:
        """
        self.dec_next_index(rpc_msg.node_id)

    def _follower_reply_success(self, rpc_msg):
        """

        :param rpc_msg:
        :return:
        """
        last_log_index = rpc_msg["lastLogIndex"]
        self.update_next_index(rpc_msg.node_id, last_log_index + 1)
        self.update_match_index(rpc_msg.node_id, last_log_index)

    def _update_commit_index(self):
        """

        :return:
        """
        match_index = self.match_index()
        indices = []
        for _, index in match_index.items():
            indices.append(index)

        node_count = len(self.members())
        commit_index = 0
        for index, count in index_map.items():
            if count >= (node_count / 2 + 1):
                commit_index = index
                break

        if commit_index == 0:
            return

        log_entry = self.get_log_entry(commit_index)
        assert log_entry

        if log_entry.term != self.current_term:
            return

        if commit_index <= self.commit_index:
            return

        self.commit_index = commit_index
        self.apply_log_entries()
