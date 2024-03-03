# coding: utf-8

HEARTBEAT = 0
LOG_REPLICATION = 1


class LogReplication(object):

    def __init__(self, local_addr, cluster_members, loop, raft):

        self._local_addr = local_addr
        self._cluster_members = cluster_members
        self._loop = loop
        self._raft = raft

        self._paused = False

        self._next_index = {}
        self._matched_index = {}

        # Keep last contact time with followers
        self._last_contact = {}

        self._replication_q = self._loop.create_fifo_queue()

    def init(self):
        """init log replication next index and matched index"""
        last_index, _ = self._raft.get_last_log()

        for addr in self._cluster_members:
            # set next index to last_index + 1
            self._next_index[addr] = last_index + 1
            # set matched index to 0
            self._matched_index[addr] = 0
            # set last contact time
            self._last_contact[addr] = self._loop.time()

    def last_contact(self):
        """return last contact time with all followers"""
        return self._last_contact

    def start_heartbeat(self):
        """send heartbeat to all followers"""
        self._loop.create_task(self._heartbeat())

    async def _heartbeat(self):
        """send heartbeat to all followers"""
        while True:
            if not self._raft.is_leader():
                break

            for addr in self._cluster_members:
                await self._replication_q.put((addr, HEARTBEAT))

            await self._loop.sleep(2.0)

    async def replicate(self):
        """send append entries request to all followers"""
        for addr in self._cluster_members:
            await self.replicate_to(addr)

    async def replicate_to(self, addr):
        """send append entries request to follower by addr"""
        await self._replication_q.put((addr, LOG_REPLICATION))

    def pause(self):
        """stop log replication"""
        self._paused = True

    def consume(self):
        """start log replication"""
        self._paused = False

    async def process_append_entries_resp(self, addr, resp):
        """process append entries resp and return commit index"""
        # update follower last activity time
        self._last_contact[addr] = self._loop.time()

        if not resp["succeed"]:
            last_index = resp["lastLogIndex"]
            self._next_index[addr] = min(last_index, self._next_index[addr]) - 1
        else:
            self._update_next_and_matched_index(addr, resp["lastAppendIndex"])

        last_index, _ = self._raft.get_last_log()
        if self._next_index[addr] != (last_index + 1):
            await self.replicate_to(addr)

        return self._update_leader_commit()

    def _update_next_and_matched_index(self, addr, last_append_index):
        """update follower matched index and next index when replicate log succeed"""
        if last_append_index > 0:
            self._next_index[addr] = last_append_index + 1

            prev_matched_index = self._matched_index[addr]
            self._matched_index[addr] = max(prev_matched_index, last_append_index)

    def _update_leader_commit(self):
        """update leader commit index"""
        matched_index = list(self._matched_index.values())
        matched_index.sort(reverse=True)

        quorum_size = self._raft.quorum_size()
        log_index = matched_index[quorum_size - 1]

        log_entry = self._raft.get_log_entry(log_index)
        assert log_entry is not None, f"can not find log entry by index: {log_index}"

        return log_index if log_entry.term == self._raft.get_current_term() else 0

    async def run_log_replication(self):
        """long-running coroutine, send log/snapshot to followers"""
        while True:
            addr, op = await self._replication_q.get()
            if self._paused:
                continue

            if addr == self._local_addr:
                # update self last contact time and matched index
                self._raft.update_last_contact()
                last_index, _ = self._raft.get_last_log()
                self._matched_index[self._local_addr] = last_index
                continue

            next_idx = self._next_index[addr]

            if op == HEARTBEAT:
                await self._send_heartbeat_request(addr, next_idx)
                continue

            first_index, _ = self._raft.get_first_log()
            if next_idx < first_index:
                # follower log is too far behind us, we should send last snapshot
                await self._send_install_snapshot_request()
            else:
                log_entries = await self._raft.log_store.find_many()
                await self._send_append_entries_request(addr, log_entries)

    async def _send_heartbeat_request(self, addr, next_index):
        """send heartbeat to follower"""
        prev_index, prev_term = await self._prepare_prev_log(next_index)
        log_entries = []
        await self._submit_append_entries_request(addr, log_entries, prev_index, prev_term)

    async def _send_append_entries_request(self, addr, next_index):
        """send append entries request to follower"""
        prev_index, prev_term = await self._prepare_prev_log(next_index)
        log_entries = await self._prepare_log_entries(next_index)
        await self._submit_append_entries_request(addr, log_entries, prev_index, prev_term)

    async def _prepare_prev_log(self, next_index):
        """"""
        snapshot_meta = await self._raft.get_last_snapshot()
        if next_index == 1:
            prev_index = 0
            prev_term = 0
        elif next_index - 1 == snapshot_meta.index:
            prev_index = snapshot_meta.index
            prev_term = snapshot_meta.term
        else:
            log_entry = await self._raft.get_log_entry(next_index - 1)
            prev_index = log_entry.index
            prev_term = log_entry.term

        return prev_index, prev_term

    async def _prepare_log_entries(self, next_index):
        """"""
        max_append_entries = self._raft.options.max_append_entries
        last_index, _ = self._raft.get_last_log()
        max_index = min(next_index + max_append_entries, last_index)

        log_entries = await self._raft.log_store.find_many(next_index, max_index)
        return [log.as_json() for log in log_entries]

    async def _submit_append_entries_request(self, addr, log_entries, prev_index, prev_term):
        """submit append entries request to tcp channel"""

        req = {
            "term": self._raft.get_current_term(),
            "entries": log_entries,
            "preLogIndex": prev_index,
            "preLogTerm": prev_term,
            "leaderCommitIndex": self._raft.get_commit_index()
        }

        await self._raft.tcp_channel.send_append_entries_request(addr, req)

    async def _send_install_snapshot_request(self):
        """send install snapshot request to follower"""
        # todo: install snapshot
