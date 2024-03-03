# coding: utf-8


class Election(object):

    def __init__(self, local_addr, cluster_members, raft):

        self._local_addr = local_addr
        self._cluster_members = cluster_members
        self._loop = raft.asyncio_loop
        self._raft = raft

        self._vote_granted = 0
        self._election_timer = None

    def start(self):
        """election entrypoint, run election coroutine"""
        self._loop.create_task(self._start_election())

    async def _start_election(self):
        """stat election"""
        self._vote_granted = 0

        current_term = self._raft.get_current_term()
        await self._raft.update_current_term(current_term + 1)
        current_term = self._raft.get_current_term()

        last_index, last_term = self._raft.get_last_log()

        req = {
            "term": current_term,
            "candidateId": self._local_addr,
            "lastLogIndex": last_index,
            "lastLogTerm": last_term,
        }

        tcp_channel = self._raft.tcp_channel
        for addr in self._cluster_members:
            if addr == self._local_addr:
                self._vote_granted += 1
                await self._raft.persist_last_vote(current_term, self._local_addr)
                continue

            await tcp_channel.send_request_vote_request(addr, req)

        # maybe we stand alone
        if self._maybe_we_win():
            await self._win_election()
            return

        self._set_election_timer()

    def inc_granted(self):
        """inc vote granted"""
        self._vote_granted += 1

        # check if we win the election
        self._maybe_we_win()

    def _maybe_we_win(self):
        """return True if we win election"""
        if self._vote_granted >= self._raft.quorum_size():
            return True

        return False

    async def _win_election(self):
        """called when win election"""
        if self._election_timer:
            self._election_timer.cancel()

        # todo: run raft with leader

    def _set_election_timer(self):
        """set election timer"""
        timeout = self._raft.options.election_timeout
        self._election_timer = self._loop.call_later(timeout, self._on_election_timeout)

    def _on_election_timeout(self):
        """when election timeout, we start election again"""
        # if we are not candidate, just return
        if not self._raft.is_candidate():
            return

        self.start()
