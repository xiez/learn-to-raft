from typing import Dict
from dataclasses import dataclass
import time
from datetime import datetime
import random
import threading
from multiprocessing import current_process

from conf import logging, PLAY_SPEED

STATE_F = 'follower'
STATE_C = 'candidate'
STATE_L = 'leader'

@dataclass
class RequestVoteArgs:
    ...

@dataclass
class RequestVoteReply:
    term: int
    vote_granted: bool

    def __str__(self):
        return f"RequestVoteReply(term:{self.term}, vote_granted:{self.vote_granted})"

@dataclass
class AppendEntriesArgs:
    ...

@dataclass
class AppendEntriesReply:
    term: int
    success: bool
    follower_id: int

    def __str__(self):
        return f"AppendEntriesReply(term:{self.term}, success:{self.success}, follower_id:{self.follower_id})"
    
@dataclass
class LogEntry:
    """Each log entry stores a state machine command along with the term number when the entry was received by
    the leader.
    Each log entry also has an integer index identifying its position in the log.

    """
    term: int
    cmd: str
    index: int

    def __str__(self):
        return f"LogEntry(term:{self.term}, cmd:{self.cmd}, index:{self.index})"

    @classmethod
    def from_dict(cls, dic):
        return cls(term=dic['term'], cmd=dic['cmd'], index=dic['index'])

class ConsensusModule:
    def __init__(self, server):
        self._lock = threading.Lock()
        self.server_id = server.server_id
        self.server = server

        self.state = STATE_F
        self.election_reset_event = -1

        self.commit_log = server.commit_log
        self.new_commit_ready = threading.Event()

        ## Persistent state on all servers:
        # (Updated on stable storage before responding to RPCs)
        self.current_term = 0
        self.vote_for = -1
        self.log = []

        ## Volatile state on all servers
        # index of highest log entry known to be committed
        self.commit_index = -1
        # index of highest log entry applied to state machine
        # (initialized to 0, increases monotonically)
        self.last_applied = -1

        ## Volatile state on Leaders
        # next index of peer servers log entries
        self.next_index: Dict[int, int] = {}
        # index of highest log entry that two logs match
        self.match_index: Dict[int, int] = {}

    def _log(self, func, msg):
        func(f"pid: {current_process().pid} server#{self.server_id} {msg}")

    def debug(self, msg):
        self._log(logging.debug, msg)

    def info(self, msg):
        self._log(logging.info, msg)

    def error(self, msg):
        self._log(logging.error, msg)
        
    def run(self):
        self.become_follower(self.current_term)

        self.run_new_commit_sender()

    def set_follower_timer(self):
        """Follower timer, also known as election timer. The timer exits either:

        - we find the election timer is no longer needed, or
        - the timer expires and this CM becomes a candidate

        """
        def ticker():
            is_running = True
            while is_running:
                time.sleep(0.01 / PLAY_SPEED)
                self.debug(f'follower timer ticking ... term={self.current_term}')

                self._lock.acquire()
                if self.state != STATE_F:
                    self.info(f"follower timer stopped, reason: current state is {self.state}, term={self.current_term}")
                    self._lock.release()
                    is_running = False
                    continue

                if self.current_term != term_started:
                    self.info(f"follower timer stopped, reason: term changed from {term_started} to {self.current_term}")
                    self._lock.release()
                    is_running = False
                    continue
 
                if time.time() - self.election_reset_event > timeout_duration:
                    self._lock.release()
                    self.start_election()
                    is_running = False
                    continue
                self._lock.release()

        with self._lock:
            timeout_duration = self.election_timeout
            term_started = self.current_term
            self.election_reset_event = time.time()

        threading.Thread(target=ticker).start()
        self.info(f'follower timer started, timeout={timeout_duration}, term={term_started}')

    @property
    def election_timeout(self):
        # make sure the timeout is much larger than broadcast time
        return random.randint(150, 300) / 1000 / PLAY_SPEED              # seconds

    @property
    def last_log_index_and_term(self):
        if len(self.log) == 0:
            return -1, -1

        with self._lock:
            return self.log[-1].index, self.log[-1].term

    def start_election(self):
        self.info(f'election timer timed out, no visible leader, start leader election')

        # To begin an election, a follower increments its current term and transitions to candidate state.
        with self._lock:
            self.state = STATE_C
            self.current_term += 1
            self.vote_for = self.server_id
            self.info(f'became Candidate at term: {self.current_term}, and vote for self')

        votes_received = 1      # vote for self
        votes_lock = threading.Lock()

        # It then votes for itself and issues RequestVote RPCs in parallel to each of the other servers in the cluster.
        # A candidate continues in this state until one of three things happens:
        # (a) it wins the election,
        # (b) another server establishes itself as leader, or
        # (c) a period of time goes by with no winner.
        
        def request_vote(pid, sp):
            last_log_index, last_log_term = self.last_log_index_and_term

            self.info(f'sending RequestVote to server#{pid} with args: '
                      f'term={self.current_term}, server_id={self.server_id} '
                      f'last_log_idx={last_log_index}, last_log_term={last_log_term}')
            try:
                reply = sp.request_vote(self.current_term, self.server_id,
                                        last_log_index, last_log_term)
                if self.current_term < reply['term']:
                    # current term out of date, become follower
                    self.become_follower(reply['term'])
                else:
                    if reply['vote_granted'] is True:
                        if self.state in [STATE_F, STATE_L]:
                            # current state is not Candidate, ignore the result
                            return
                        
                        nonlocal votes_received
                        with votes_lock:
                            votes_received += 1
                            if votes_received * 2 > len(self.server.peer_servers) + 1:
                                self.info(f'won election with {votes_received} votes')
                                self.become_leader()
            except Exception as e:
                err = f"RV RPC error to server#{pid}: {e}"
                print(err)
                self.info(err)

        for pid, sp in self.server.peer_clients.items():
            threading.Thread(target=request_vote, args=(pid, sp)).start()

        self.set_candidate_timer()

    def become_follower(self, term):
        """If RPC request or response contains term T > currentTerm: set currentTerm = T,
        convert to follower.

        New follower timer is set, the old one will be stopped during the next tick.
        """
        with self._lock:
            self.state = STATE_F
            prev_term = self.current_term
            self.current_term = term
            self.vote_for = -1
            self.info(f"became Follower at term:{term}, prev_term: {prev_term}, start new follower timer")

        self.set_follower_timer()
        
    def set_candidate_timer(self):
        """Almost same as follower timer.

        Maybe we should combine those two timers ?
        """
        def ticker():
            is_running = True
            while is_running:
                time.sleep(0.01 / PLAY_SPEED)
                self.debug(f'candidate timer ticking ... term={self.current_term}')

                self._lock.acquire()
                if self.state != STATE_C:
                    self.info(f"candidate timer stoped, reason: current state is not Candidate, term={self.current_term}")
                    self._lock.release()
                    is_running = False
                    continue

                if self.current_term != term_started:
                    self.info(f"candidate timer stoped, reason: term changed from {term_started} to {self.current_term}")
                    self._lock.release()
                    is_running = False
                    continue

                if time.time() - self.election_reset_event > timeout_duration:
                    self._lock.release()
                    self.start_election()
                    is_running = False
                    continue
                self._lock.release()

        with self._lock:
            timeout_duration = self.election_timeout
            term_started = self.current_term
            self.election_reset_event = time.time()
                    
        threading.Thread(target=ticker).start()
        self.info(f'candidate timer started with timeout {timeout_duration}, term={term_started}')

    def become_leader(self):
        if self.state == STATE_L:
            return

        self.state = STATE_L
        self.info(f"became Leader at term:{self.current_term}, sending heartbeats ...")

        # when this server becomes the leader, the next index of peers log must assign
        # current server
        for pid, _ in self.server.peer_clients.items():
            self.next_index[pid] = len(self.log)
            self.match_index[pid] = -1

        def ticker():
            is_running = True
            while is_running:
                self.send_heartbeats()

                time.sleep(0.05 / PLAY_SPEED)
                if self.state != STATE_L:
                    is_running = False

        threading.Thread(target=ticker).start()

    def send_heartbeats(self):
        def ae(pid, sp):
            with self._lock:
                term = self.current_term
                leader_id = self.server_id

                # next index of log of the peer
                ni = self.next_index[pid]

                # preceding log entry of newly appended log
                prev_log_index = ni - 1
                try:
                    prev_log = self.log[prev_log_index]
                    prev_log_term = prev_log.term
                except IndexError:
                    prev_log_term = -1

                entries = self.log[ni:]  
                leader_commit_index = self.commit_index

            self.info(f'sending AppendEntries to server#{pid} with args: term={self.current_term}, '
                      f'leader_id={self.server_id}, prev_log_index:{prev_log_index}, prev_log_term:{prev_log_term}, '
                      f'entries:{entries}, leader_commit_index:{leader_commit_index}')
            try:
                reply = sp.append_entries(term, leader_id, prev_log_index,
                                          prev_log_term, entries, leader_commit_index)
            except Exception as e:
                err = f"AE RPC error to server#{pid}: {e}"
                print(e)
                self.info(err)
                return

            # TODO: with lock

            if self.current_term < reply['term']:
                # stop heartbeats and step down
                self.become_follower(reply['term'])
            else:
                if self.state != STATE_L:
                    return

                ## follow the algorithm in Figure 2 of the paper (Rules for Servers)
                if not reply['success']:
                    # If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
                    self.next_index[pid] -= 1
                    return

                # If successful: update nextIndex and matchIndex for follower
                self.next_index[pid] = ni + len(entries)
                self.match_index[pid] = self.next_index[pid] - 1

                # If there exists an N such that N > commitIndex, a majority
                # of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
                saved_commit_index = self.commit_index
                for i in range(self.commit_index + 1, len(self.log)):
                    match_count = 1

                    # 5.4.2 safety guarantee - at least one new entry from leader's term must also be stored
                    # on majority of servers
                    if self.log[i].term != self.current_term:
                        break

                    for pid, host_port in self.server.peer_servers:
                        if self.match_index[pid] >= i:
                            match_count += 1

                        if match_count * 2 > len(self.server.peer_servers) + 1:
                            self.commit_index = i

                if self.commit_index != saved_commit_index:
                    # newly commit log
                    self.info(f'leader sets commitIndex to {self.commit_index}')
                    self.new_commit_ready.set()  # notify background thread that a new commit is ready to consume

        for pid, sp in self.server.peer_clients.items():
            threading.Thread(target=ae, args=(pid, sp)).start()

    def submit(self, cmd):
        """Each client request contains a command to be executed by the replicated state machines.
        The leader appends the command to its log as a new entry, then issues
        AppendEntries RPCs in parallel to each of the other servers to replicate the entry.
        """
        self.info(f'submiting command: {cmd}')
        with self._lock:
            if self.state != STATE_L:
                return False

            en = LogEntry(term=self.current_term, cmd=cmd, index=len(self.log))
            self.log.append(en)
            return True

    def run_new_commit_sender(self):
        """Add new log entries to server's commit log"""
        def f():
            while True:
                self.new_commit_ready.wait()  # block until new entries arrived
                self.info(f'start to consume new commit logs, term={self.current_term}, last_applied={self.last_applied}, commit_index={self.commit_index}')
 
                with self._lock:
                    # saved_term = self.current_term
                    # saved_last_applied = self.last_applied
                    if self.commit_index > self.last_applied:
                        entries = self.log[self.last_applied + 1: self.commit_index + 1]
                        self.last_applied = self.commit_index
                    else:
                        entries = []
 
                self.info(f"update server's commit log with entries: {entries}")
 
                for entry in entries:
                    self.commit_log.put(entry)
 
                self.new_commit_ready.clear()

        threading.Thread(target=f).start()
        self.info("start background watcher to add new log entries to server's commit log")


    ########## RPCs
    def request_vote(self, term, server_id, last_log_index, last_log_term):
        self.info(f"got RequestVote from server#{server_id} at term: {term}")

        if self.current_term < term:
            self.info(f"... current_term is out of date, current: {self.current_term}, request: {term}")
            if self.state in [STATE_L, STATE_C, STATE_F]:
                self.become_follower(term)

        # voter's last log index and term
        v_last_log_index, v_last_log_term = self.last_log_index_and_term

        if self.current_term == term:
            ## 5.4.1 safety guarantee - leader restriction
            # If candidate's log is "more complete" than voter's log, then the vote
            # is success. "more complete" can be defined as:
            # 1. last term of voter's log is smaller than candidate's log or
            # 2. if the terms match, voter's log is not longer than candidate's log
            if self.vote_for == -1 and \
               (v_last_log_term < last_log_term or \
                (v_last_log_term == last_log_term and v_last_log_index <= last_log_index)):
                self.vote_for = server_id
                reply = RequestVoteReply(term, True)
            else:
                reply = RequestVoteReply(term, False)
        elif self.current_term > term:
            reply = RequestVoteReply(self.current_term, False)
        else:
            ...

        self.info(f'... RequestVote reply {reply}')
        return reply

    def append_entries(self, term, leader_id, prev_log_index,
                       prev_log_term, entries, leader_commit_index):
        entries = [LogEntry.from_dict(e) for e in entries]

        self.info(f"got AppendEntries from server#{leader_id} at term: {term}, prev_log_index: {prev_log_index},"
                  f"prev_log_term: {prev_log_term}, entries: {entries}, leader_commit_index: {leader_commit_index}")

        if self.current_term < term:
            self.info(f"... current_term is out of date, current: {self.current_term}, request: {term}")
            if self.state in [STATE_L, STATE_C, STATE_F]:
                self.become_follower(term)

        reply = AppendEntriesReply(term, False, self.server_id)
        if self.current_term > term:
            # 1. Reply false if term < currentTerm
            reply = AppendEntriesReply(self.current_term, False, self.server_id)
        
        elif self.current_term == term:
            if self.state == STATE_F:
                # reset timer when got heartbeats from Leader
                self.election_reset_event = time.time()
                self.info(f"reset election event")

            reply = AppendEntriesReply(self.current_term, True, self.server_id)

            ## Verify the last log entry
            # 2. Reply false if log doesn’t contain an entry at prevLogIndex
            # whose term matches prevLogTerm
            process_entries = True
            if prev_log_index > -1:
                if self.log[prev_log_index].term == prev_log_term:
                    # prev_log matches, proceed to process new entries (if any)
                    ...
                else:
                    # prev_log conflict, notify leader to send the log before prev_log
                    self.info(f"prev_log at index:{prev_log_index} conflict, term: {self.log[prev_log_index].term}, "
                              f"request: {prev_log_term}")
                    process_entries = False
                    reply = AppendEntriesReply(self.current_term, False, self.server_id)

            ## Process any new entries
            if process_entries is True:
                # 3. If an existing entry conflicts with a new one (same index
                # but different terms), delete the existing entry and all that
                # follow it
                new_entries = []
                for en in entries:
                    try:
                        if self.log[en.index].term == en.term:
                            # duplicated entry, skip
                            ...
                        else:
                            # conflict entry, delete existing entry and all that follow it
                            del self.log[en.index:]
                            new_entries.append(en)
                    except IndexError:
                        new_entries.append(en)

                # 4. Append any new entries not already in the log
                self.log += new_entries
                if new_entries:
                    self.info(f"after append new log entries: {self.log}")

                # 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
                if leader_commit_index > self.commit_index:
                    last_log_index, _ = self.last_log_index_and_term
                    self.commit_index = min(leader_commit_index, last_log_index)

        else:
            # should never reach here
            self.error("SHOULD NEVER REACH HERE")

        self.info(f'... AppendEntries reply {reply}')
        return reply
