# coding: utf-8

import os
import typing
import random

from .storage import Storage
from . import roles
from .models import Rpc

from libreactor.event_loop_thread import EventLoopThread


class Raft(object):

    def __init__(self, conf_file, rpc_service):

        self._conf_file = conf_file
        self._rpc_service = rpc_service

        self._current_term = -1
        self._vote_for = -1
        self._log_entries = []

        self._commit_index = -1
        self._last_applied = -1
        self._last_log_index = -1
        self._last_log_term = -1

        self._next_index = {}
        self._match_index = {}

        self._my_role: typing.Union[roles.Role, None] = None
        self._my_id = -1
        self._my_name = os.uname()[1]

        # id -> ip
        self._node_map = {}

        # ["host1", "host2"]
        self._node_list = []

        # id list
        self._quorum_set = set()

        self._storage = Storage()

        ev_t = EventLoopThread()
        ev_t.start()

        self._ev = ev_t.get_event_loop()

        self._heartbeat_interval = 0.2

        self._election_timeout_timer = None
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

    def last_log_index(self):
        """

        :return:
        """
        return self._last_log_index

    def last_log_term(self):
        """

        :return:
        """
        return self._last_log_term

    def init_match_index(self):
        """

        :return:
        """

    def set_match_index(self, mem_id, log_idx):
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

    def set_next_index(self, mem_id, log_idx):
        """

        :param mem_id:
        :param log_idx:
        :return:
        """
        self._next_index[mem_id] = log_idx

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

    def to_follower(self):
        """

        :return:
        """
        self._my_role = roles.Follower(self)
        self._my_role.boostrap()

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
        self._commit_index = self._storage.get_uint64("commitIndex", 0)
        self._last_applied = self._storage.get_uint64("lastApplied", 0)
        self._current_term = self._storage.get_uint64("currentTerm", 0)
        self._vote_for = self._storage.get_int64("voteFor", -1)

    @property
    def commit_index(self):
        """

        :return:
        """
        return self._commit_index

    @commit_index.setter
    def commit_index(self, commit_idx):
        """

        :param commit_idx:
        :return:
        """
        self._commit_index = commit_idx
        self._storage.put_uint64("commitIndex", commit_idx)

    @property
    def last_applied(self):
        """

        :return:
        """
        return self._last_applied

    @last_applied.setter
    def last_applied(self, last_idx):
        """

        :param last_idx:
        :return:
        """
        self._last_applied = last_idx
        self._storage.put_uint64("lastApplied", last_idx)

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
    def vote_for(self, vote_for):
        """

        :param vote_for:
        :return:
        """
        self._vote_for = vote_for

    def apply_log_entries(self):
        """

        :return:
        """
        if self._commit_index > self._last_applied:
            pass

    def save_log_entries(self, log_entries):
        """

        :param log_entries:
        :return:
        """

    def get_log_entry(self, log_index):
        """

        :param log_index:
        :return:
        """
        # todo
        self.storage().get()

    def delete_log_entry(self, log_index):
        """

        :param log_index:
        :return:
        """

    def delete_log_entries(self, log_start_index):
        """

        :param log_start_index:
        :return:
        """

    def dispatch_rpc(self, rpc):
        """

        :param rpc:
        :return:
        """
        if rpc.op == Rpc.REQUEST_VOTE:
            self._my_role.process_vote_rpc(rpc)
        elif rpc.op == Rpc.REQUEST_VOTE_ACK:
            self._my_role.process_vote_ack(rpc)
        elif rpc.op == Rpc.APPEND_ENTRIES:
            self._my_role.process_append_entries_rpc(rpc)
        elif rpc.op == Rpc.APPEND_ENTRIES_ACK:
            self._my_role.process_append_entries_ack(rpc)
        else:
            pass

    def start_election(self):
        """

        :return:
        """
        assert self._my_role.is_candidate()

        self.current_term += 1

        self.request_vote()

    def request_vote(self):
        """request members to vote us as leader

        :return:
        """
        # vote for self
        self.vote_for = self.my_id()

        vote_msg = {
            "term": self.current_term,
            "candidateId": self.my_id(),
            "lastLogIndex": self.last_log_index(),
            "lastLogTerm": self.last_log_term(),
        }

        # todo: send vote rpc to all members

    def set_election_timeout(self):
        """set election timeout, timeout is between 150 - 300 msec

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

    def send_heartbeat(self):
        """leader send heartbeat to members

        :return:
        """

    def stop_heartbeat(self):
        """leader stop heartbeat with members

        :return:
        """
        assert self._my_role.is_leader()

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
            "leaderId": self.my_id(),
            "prevLogIndex": -1,
            "prevLogTerm": -1,
            "entries": [],
            "leaderCommit": -1,
        }
