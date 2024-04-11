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
class RequestVoteReply:
    term: int
    vote_granted: bool

    def __str__(self):
        return f"RequestVoteReply(term:{self.term}, vote_granted:{self.vote_granted})"

@dataclass
class AppendEntriesReply:
    term: int
    success: bool
    follower_id: int

    def __str__(self):
        return f"AppendEntriesReply(term:{self.term}, success:{self.success}, follower_id:{self.follower_id})"
    

class ConsensusModule:
    def __init__(self, server):
        self._lock = threading.Lock()
        self.server_id = server.server_id
        self.server = server

        self.state = STATE_F
        self.election_reset_event = -1

        # Persistent state on all servers:
        # (Updated on stable storage before responding to RPCs)
        self.current_term = 0
        self.vote_for = -1

    def log(self, func, msg):
        func(f"pid: {current_process().pid} server#{self.server_id} {msg}")

    def debug(self, msg):
        self.log(logging.debug, msg)

    def info(self, msg):
        self.log(logging.info, msg)

    def run(self):
        self.become_follower(self.current_term)

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
        return random.randint(150, 300) / 1000 / PLAY_SPEED              # seconds
        
    def start_election(self):
        self.info(f'election timer timed out, no visible leader, start leader election')

        # To begin an election, a follower increments its current term and transitions to candidate state.
        with self._lock:
            self.state = STATE_C
            self.current_term += 1
            self.vote_for = self.server_id
            self.info(f'became Candidate at term: {self.current_term}, and vote for self')

        votes_received = 1
        votes_lock = threading.Lock()

        # It then votes for itself and issues RequestVote RPCs in parallel to each of the other servers in the cluster.
        # A candidate continues in this state until one of three things happens:
        # (a) it wins the election,
        # (b) another server establishes itself as leader, or
        # (c) a period of time goes by with no winner.
        
        def request_vote(pid, sp):
            self.info(f'sending RequestVote to server#{pid} with args: term={self.current_term}, server_id={self.server_id}')
            try:
                reply = sp.request_vote(self.current_term, self.server_id)
                if self.current_term < reply['term']:
                    # current term out of date, become follower
                    self.become_follower(reply['term'])
                else:
                    if reply['vote_granted'] is True:
                        if self.state in [STATE_F, STATE_L]:
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
            self.info(f'sending AppendEntries to server#{pid} with args: term={self.current_term}, leader_id={self.server_id}')
            try:
                reply = sp.append_entries(self.current_term, self.server_id)
                if self.current_term < reply['term']:
                    # stop heartbeats and step down
                    self.become_follower(reply['term'])
                else:
                    ...
            except Exception as e:
                err = f"AE RPC error to server#{pid}: {e}"
                print(err)
                self.info(err)

        for pid, sp in self.server.peer_clients.items():
            threading.Thread(target=ae, args=(pid, sp)).start()

    ########## RPCs
    def request_vote(self, term, server_id):
        self.info(f"got RequestVote from server#{server_id} at term: {term}")

        if self.current_term < term:
            self.info(f"... current_term is out of date, current: {self.current_term}, request: {term}")
            if self.state in [STATE_L, STATE_C, STATE_F]:
                self.become_follower(term)

        if self.current_term == term:
            if self.vote_for == -1:
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

    def append_entries(self, term, leader_id):
        self.info(f"got AppendEntries from server#{leader_id} at term: {term}")

        if self.current_term < term:
            self.info(f"... current_term is out of date, current: {self.current_term}, request: {term}")
            if self.state in [STATE_L, STATE_C, STATE_F]:
                self.become_follower(term)

        reply = AppendEntriesReply(term, False, self.server_id)
        if self.current_term == term:
            reply.success = True
            if self.state == STATE_F:
                # reset timer when got heartbeats from Leader
                self.election_reset_event = time.time()
                self.info(f"reset election event")
        elif self.current_term > term:
            reply = AppendEntriesReply(self.current_term, False, self.server_id)
        else:
            ...

        self.debug(f'... AppendEntries reply {reply}')
        return reply
