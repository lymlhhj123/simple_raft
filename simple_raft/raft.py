# coding: utf-8

import json
import logging
from collections import deque

from . import const
from . import log
from .log_replication import LogReplication
from .tcp_channel import TcpChannel
from .election import Election

logger = logging.getLogger()


class Raft(object):

    def __init__(self, loop, local_addr, cluster_members, fsm_store, log_store, stable_store, snapshot_store, options):

        self._local_id = local_addr
        self._local_addr = local_addr
        self._cluster_members = cluster_members
        self._loop = loop
        self._options = options

        self._commit_index = 0
        self._current_term = 0

        self._leader_addr = ""
        self._leader_id = ""

        self._fsm_store = fsm_store
        self._log_store = log_store
        self._stable_store = stable_store
        self._snapshot_store = snapshot_store

        self._state = const.RAFT_FOLLOWER

        # keep last time contact with leader
        self._last_contact = 0

        # first raft log
        self._first_term = 0
        self._first_index = 0

        # last raft log
        self._last_term = 0
        self._last_index = 0

        self._commit_q = loop.create_fifo_queue()
        self._fsm_q = loop.create_fifo_queue()
        self._snapshot_q = loop.create_fifo_queue()

        # used by leader, store in-flight log entries which wait to commit
        self._flight_entries = deque()

        self._tcp_channel = TcpChannel(local_addr, cluster_members, self)
        self._log_replication = LogReplication(local_addr, cluster_members, self)
        self._leader_election = Election(local_addr, cluster_members, self)

        # follower timer
        self._heartbeat_timer = None

        # leader timer
        self._lease_check_timer = None

    @property
    def asyncio_loop(self):
        """return event loop"""
        return self._loop

    @property
    def options(self):
        """return raft options"""
        return self._options

    @property
    def cluster_members(self):
        """return raft cluster members"""
        return self._cluster_members

    @property
    def tcp_channel(self):
        """return tcp channel"""
        return self._tcp_channel

    @property
    def log_replication(self):
        """return log replication"""
        return self._log_replication

    @property
    def fsm_store(self):
        """return fsm which log apply to"""
        return self._fsm_store

    @property
    def log_store(self):
        """return log store"""
        return self._log_store

    @property
    def stable_store(self):
        """return stable store"""
        return self._stable_store

    @property
    def local_addr(self):
        """"""
        return self._local_addr

    @property
    def local_id(self):
        """"""
        return self._local_id

    @property
    def snapshot_store(self):
        """return snapshot store"""
        return self._snapshot_store

    def get_default_rpc_headers(self):
        """get rpc default headers"""
        headers = {
            "server_id": self.local_id,
            "server_addr": self.local_addr,
        }

        return headers

    def quorum_size(self):
        """return quorum member size"""
        return len(self.cluster_members) / 2 + 1

    def is_leader(self):
        """return True if we are leader"""
        return self.get_state() == const.RAFT_LEADER

    def is_follower(self):
        """return True if we are follower"""
        return self.get_state() == const.RAFT_FOLLOWER

    def is_candidate(self):
        """return True if we are candidate"""
        return self.get_state() == const.RAFT_CANDIDATE

    def get_state(self):
        """return raft state"""
        return self._state

    def set_state(self, raft_state):
        """set raft state"""
        old, self._state = self._state, raft_state
        return old

    def get_leader(self):
        """return leader as we have known so far"""
        return self._leader_id, self._leader_addr

    def set_leader(self, leader_addr, leader_id):
        """set leader"""
        self._leader_addr = leader_addr
        self._leader_id = leader_id

    def get_last_contact(self):
        """get last contact time"""
        return self._last_contact

    def update_last_contact(self):
        """last contact time received message from leader"""
        self._last_contact = self._loop.time()

    def get_current_term(self):
        """get current term"""
        return self._current_term

    async def update_current_term(self, term):
        """update current term to `term`"""
        assert term > self._current_term
        self._current_term = term

        # persist current term to disk
        await self.stable_store.put_int(const.CURRENT_TERM, self._current_term)

        # we also clear leader
        self.set_leader("", "")

    def get_commit_index(self):
        """get committed index"""
        return self._commit_index

    def set_commit_index(self, idx):
        """set committed index"""
        assert idx > self._commit_index
        self._commit_index = idx

    def get_first_log(self):
        """get first log index and term"""
        return self._first_index, self._first_term

    def set_first_log(self, log_index, log_term):
        """set first log index and term"""
        self._first_index = log_index
        self._first_term = log_term

    def get_last_log(self):
        """get last log index and term"""
        return self._last_index, self._last_term

    def set_last_log(self, log_index, log_term):
        """set last log index and term"""
        self._last_index = log_index
        self._last_term = log_term

    async def get_log_entry(self, log_index):
        """get log entry"""
        return await self.log_store.find_one(log_index)

    async def persist_last_vote(self, voted_term, voted_candidate):
        """persist last vote info"""
        data = {
            const.LAST_VOTE_TERM: voted_term,
            const.LAST_VOTE_CANDIDATE: voted_candidate
        }
        await self.stable_store.put(const.LAST_VOTE_KEY, json.dumps(data))

    async def get_last_vote(self):
        """get last vote term and candidate"""
        data = await self.stable_store.get(const.LAST_VOTE_KEY)
        vote_info = json.load(data) if data else {}

        last_vote_term = vote_info.get(const.LAST_VOTE_TERM, 0)
        last_vote_candidate = vote_info.get(const.LAST_VOTE_CANDIDATE, "")
        return last_vote_term, last_vote_candidate

    async def get_last_snapshot(self):
        """return last snapshot meta"""
        snapshots = await self.list_snapshots()

        # index 0 is last snapshot
        return snapshots[0] if snapshots else None

    async def list_snapshots(self):
        """"""
        waiter = self._loop.create_future()
        await self._snapshot_q.put([waiter, const.OP_SNAPSHOT_LIST, None])

        return await waiter

    async def start_election(self):
        """start raft vote election"""
        self._leader_election.start()

    async def win_election(self):
        """called when we win election"""
        self.set_state(const.RAFT_LEADER)

        local_addr, local_id = self.local_addr, self.local_id
        self.set_leader(local_addr, local_id)

        # set next_index and matched_index
        self.log_replication.init()

        self.log_replication.start_heartbeat()

        # create noop log entry used by set commit index
        log_entry = log.new_noop_log()

        future = self.asyncio_loop.create_future()
        future_log = log.wrap_future(log_entry, future)
        await self.process_logs([future_log])

    def check_leader_lease(self):
        """if lease timeout, we step down to follower"""
        lease_timeout = self.options.leader_lease_timeout

        now = self._loop.time()
        contacted = 0
        last_contact_time = self.log_replication.last_contact()
        for addr in self.cluster_members:
            if addr == self._local_addr:
                contacted += 1
                continue

            contact_time = last_contact_time[addr]
            diff = now - contact_time
            if diff <= lease_timeout:
                contacted += 1

        # we are still leader
        if contacted >= self.quorum_size():
            return

        # we can't contact a majority of members, step down to follower
        self._step_down()

    def _step_down(self):
        """we can't contact a majority of members, step down to follower

        this method only called on leader
        """
        assert self.is_leader()

        # todo:

    async def dispatch_message(self, message):
        """dispatch rpc message"""
        if message.is_request_vote_req():
            await self.process_vote_request(message)
        elif message.is_request_vote_resp():
            await self.process_vote_request_resp(message)
        elif message.is_append_entries_req():
            await self.process_append_entries_request(message)
        elif message.is_append_entries_resp():
            await self.process_append_entries_resp(message)
        elif message.is_install_snapshot_req():
            pass
            # todo
        elif message.is_install_snapshot_resp():
            pass
            # todo
        else:
            pass

    async def process_vote_request(self, message):
        """process vote request send by candidate"""
        resp = await self._process_vote_request(message)
        await self.tcp_channel.send_request_vote_resp(message.addr(), resp)

    async def _process_vote_request(self, message):
        """process vote request"""
        current_term = self.get_current_term()
        req = message.payload

        resp = {
            "term": req["term"],
            "voteGranted": False,
        }

        if req["term"] < current_term:
            resp["term"] = current_term
            return resp

        if req["term"] > current_term:
            await self.update_current_term(req["term"])
            self.set_state(const.RAFT_FOLLOWER)

        last_voted_term, last_voted_candidate = await self.get_last_vote()

        # 1. we are leader/candidate, already vote self
        # 2. we are follower, already vote others
        if (last_voted_term == req["term"] and last_voted_candidate != ""
                and last_voted_candidate != req["candidateId"]):
            return resp

        last_index, last_term = self.get_last_log()

        if last_term > req["lastLogTerm"]:
            logger.warning(f"reject vote request since our last term is greater, "
                           f"our term: {last_term}, candidate term: {req["lastLogTerm"]}")
            return resp

        if last_term == req["lastLogTerm"] and last_index > req["lastLogIndex"]:
            logger.warning(f"reject vote request since our last index is greater, "
                           f"our index: {last_index}, candidate index: {req["lastLogIndex"]}")
            return resp

        # update last activity time
        self.update_last_contact()

        await self.persist_last_vote(req["term"], req["candidateId"])
        resp["voteGranted"] = True

        assert self.is_follower(), "we must be follower"

        return resp

    async def process_vote_request_resp(self, message):
        """process vote request resp"""
        resp = message.payload
        current_term = self.get_current_term()

        # old term, ignore
        if resp["term"] < current_term:
            return

        if resp["term"] > current_term:
            self.set_state(const.RAFT_FOLLOWER)
            await self.update_current_term(resp["term"])
            return

        # we are not candidate, just ignore
        if not self.is_candidate():
            return

        if not resp["voteGranted"]:
            return

        self._leader_election.inc_granted()

    async def process_append_entries_request(self, message):
        """process append log entries send by leader"""
        resp = await self._process_append_entries_request(message)
        await self._tcp_channel.send_append_entries_resp(message.addr(), resp)

    async def _process_append_entries_request(self, message):
        """process append log entries request from leader"""
        last_index, _ = self.get_last_log()
        current_term = self.get_current_term()
        req = message.payload

        # if heartbeat, entries is empty
        if not req["entries"]:
            last_append_index = 0
        else:
            last_append_index = req["entries"][-1].index

        resp = {
            "succeed": False,
            "lastLogIndex": last_index,
            "lastAppendIndex": last_append_index,
            "term": req["term"],
        }

        if req["term"] < current_term:
            resp["term"] = current_term
            return resp

        # update last activity time with leader
        self.update_last_contact()

        if req["term"] > current_term:
            await self.update_current_term(req["term"])

        self.set_state(const.RAFT_FOLLOWER)
        self.set_leader(req["leaderId"], req["leaderId"])

        if req["prevLogIndex"] > 0:
            local_log = await self.get_log_entry(req["prevLogIndex"])
            # prev log conflict
            if not local_log or local_log.term != req["prevLogTerm"]:
                logger.warning(f"prevLog conflict: {req["prevLogIndex"]}->{req["prevLogTerm"]}, reject request")
                return resp

        new_logs = []
        for i, entry in enumerate(req["entries"]):
            local_log = await self.get_log_entry(entry.index)
            if not local_log:
                new_logs = req["entries"][i:]
                break

            if local_log.term == entry.term:
                continue

            logger.warning(f"log conflict, delete range: [{entry.index}, {last_index}]")
            await self.log_store.delete_many(entry.index, last_index)
            new_logs = req["entries"][i:]
            break

        if new_logs:
            await self.log_store.insert_many(new_logs)
            self.set_last_log(new_logs[-1].index, new_logs[-1].term)

        last_index, _ = self.get_last_log()
        commit_index = min(req["leaderCommitIndex"], last_index)
        local_commit = self.get_commit_index()
        commit_index = max(local_commit, commit_index)

        self.set_commit_index(commit_index)
        # wakeup coroutine to apply logs
        await self._commit_q.put(commit_index)

        resp["lastLogIndex"] = last_index
        resp["succeed"] = True
        return resp

    async def process_append_entries_resp(self, message):
        """process append entries resp"""
        current_term = self.get_current_term()
        resp = message.payload

        if resp["term"] < current_term:
            return

        if resp["term"] > current_term:
            self.set_state(const.RAFT_FOLLOWER)
            await self.update_current_term(resp["term"])
            return

        assert self.is_leader(), "we must be leader here"

        leader_commit = await self.log_replication.process_append_entries_resp(message.addr(), resp)

        if leader_commit > self.get_commit_index():
            self.set_commit_index(leader_commit)
            # wakeup coroutine to apply logs
            await self._commit_q.put(leader_commit)

    async def process_install_snapshot_request(self, message):
        """"""
        # todo

    async def process_install_snapshot_resp(self, message):
        """"""
        # todo

    async def process_command(self, command):
        """process client command, wrap as future entry and process it

        this method only called on leader
        """
        assert self.is_leader(), "i am not leader"

        log_entry = log.new_command_log(command)

        future = self.asyncio_loop.create_future()
        future_entry = log.wrap_future(log_entry, future)

        await self.process_logs([future_entry])

        return await future_entry.future

    async def process_logs(self, future_entries):
        """push log to disks, make it in-flight, replicate to follower

        this method only called on leader
        """
        assert self.is_leader(), "i am not leader"

        last_index, _ = self.get_last_log()
        current_term = self.get_current_term()

        log_entries = []
        for entry in future_entries:
            last_index += 1
            entry.log_entry.index = last_index
            entry.log_entry.term = current_term
            log_entries.append(entry.log_entry)

        # push log entry to disks
        await self.log_store.insert_many(log_entries)

        # update last log info since it's on disks now
        self.set_last_log(last_index, current_term)

        # append to leader flight queue
        self._flight_entries.extend(future_entries)

        # wakeup coroutine to replicate log to followers
        await self.log_replication.replicate()

    async def run_apply(self):
        """long-running coroutine, get committed log from disk and apply to fsm"""
        max_apply_entries = self.options.max_apply_entries
        while True:
            committed_idx = await self._commit_q.get()
            applied_idx, _ = await self.fsm_store.last_apply_info()

            if committed_idx <= applied_idx:
                continue

            start = applied_idx + 1
            while True:
                end = start + max_apply_entries

                log_entries = await self.log_store.find_many(start, end)
                if not log_entries:
                    break

                waiter = self._loop.create_future()
                await self._fsm_q.put([waiter, const.OP_FSM_APPLY_LOG, log_entries])

                apply_result = await waiter

                # todo: if we are leader, set result with flight entries

                start = end + 1

    async def run_fsm(self):
        """long-running coroutine, apply committed logs to disks"""
        while True:
            waiter, op, args = await self._fsm_q.get()
            if op == const.OP_FSM_TAKE_SNAPSHOT:
                await self.fsm_store.take_snapshot()
            elif op == const.OP_FSM_APPLY_LOG:
                await self.fsm_store.apply(args)
            elif op == const.OP_FSM_RESTORE_SNAPSHOT:
                await self.fsm_store.restore_snapshot(args)
            else:
                # unknown op
                pass

    async def run_snapshot(self):
        """long-running coroutine, create snapshot"""
        while True:
            waiter, op, args = await self._snapshot_q.get()
            if op == const.OP_SNAPSHOT_USER:
                await self._task_snapshot()
            elif op == const.OP_SNAPSHOT_TIMEOUT:
                should = await self._should_snapshot()
                if should:
                    await self._task_snapshot()
            elif op == const.OP_SNAPSHOT_LIST:
                await self._snapshot_store.list_snapshot()
            elif op == const.OP_SNAPSHOT_OPEN:
                await self._snapshot_store.open_snapshot(args)
            else:
                # unknown snapshot op
                pass

    async def _should_snapshot(self):
        """return True if we should create snapshot, otherwise False"""
        first_index, _ = self.get_first_log()
        last_applied, _ = await self.fsm_store.last_apply_info()

        diff = last_applied - first_index
        return diff >= self.options.snapshot_threshold

    async def _task_snapshot(self):
        """create snapshot from fsm and save to disk"""
        waiter = self._loop.create_future()
        await self._fsm_q.put([waiter, const.OP_FSM_TAKE_SNAPSHOT, None])

        fsm_snapshot = await waiter

        # trim raft log
