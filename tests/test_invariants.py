import concurrent.futures
import queue
import random
import threading
import time
import unittest
from typing import Any, Dict, List, Optional, Tuple


# =====================================================================
# Raft Core Engine Implementation (Target Under Test)
# =====================================================================

class LogEntry:
    def __init__(self, term: int, index: int, command: Any):
        self.term = term
        self.index = index
        self.command = command

    def __repr__(self):
        return f"LogEntry(term={self.term}, index={self.index}, cmd={self.command})"


class RaftNode:
    def __init__(self, node_id: int, peer_ids: List[int]):
        self.node_id = node_id
        self.peer_ids = peer_ids
        self.all_nodes: Dict[int, 'RaftNode'] = {}

        # Persistent state
        self.current_term = 0
        self.voted_for: Optional[int] = None
        self.log: List[LogEntry] = []  # 1-indexed conceptually, 0-indexed in python

        # Volatile state on all servers
        self.commit_index = 0
        self.last_applied = 0
        self.state = "Follower"  # Follower, Candidate, Leader

        # Volatile state on leaders
        self.next_index: Dict[int, int] = {}
        self.match_index: Dict[int, int] = {}

        # Concurrency control
        self.lock = threading.RLock()
        self.apply_cond = threading.Condition(self.lock)
        
        # Communication channels (simulated via thread-safe queues)
        self.inbox: queue.Queue = queue.Queue()
        self.running = True

        # Role state counters
        self.votes_received = set()
        
        # Background worker
        self.worker_thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self.worker_thread.start()

    def stop(self):
        with self.lock:
            self.running = False
            self.inbox.put(("STOP", None))

    def _run_event_loop(self):
        while True:
            try:
                item = self.inbox.get(timeout=0.05)
                if item is None:
                    continue
                msg_type, payload = item
                if msg_type == "STOP":
                    break
                with self.lock:
                    if not self.running:
                        break
                    if msg_type == "APPEND_ENTRIES":
                        self._handle_append_entries(payload)
                    elif msg_type == "APPEND_ENTRIES_RESP":
                        self._handle_append_entries_resp(payload)
                    elif msg_type == "REQUEST_VOTE":
                        self._handle_request_vote(payload)
                    elif msg_type == "REQUEST_VOTE_RESP":
                        self._handle_request_vote_resp(payload)
            except queue.Empty:
                with self.lock:
                    if not self.running:
                        break
            except Exception:
                pass

    def deliver_message(self, msg_type: str, payload: Any):
        if self.running:
            self.inbox.put((msg_type, payload))

    # --- Client Interface ---
    def client_execute(self, command: Any) -> Tuple[bool, Optional[int]]:
        with self.lock:
            if self.state != "Leader":
                return False, None
            index = len(self.log) + 1
            entry = LogEntry(self.current_term, index, command)
            self.log.append(entry)
            self.match_index[self.node_id] = index
            self.next_index[self.node_id] = index + 1
            
            # Broadcast to peers
            self._send_append_entries_to_all()
            return True, index

    # --- Invariant Checks ---
    def verify_invariants(self):
        with self.lock:
            # 1. Election Safety: at most one leader can be elected in a given term.
            # (Enforced globally or checked via state)
            
            # 2. Leader Append-Only: a leader never overwrites or truncates its entries.
            # 3. Log Matching: if two logs contain an entry with the same index and term, then they are identical.
            # 4. Leader Completeness: if a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms.
            # 5. State Machine Safety: if a server has applied a given log entry to its state machine, no other server will ever apply a different log entry for the same index.
            assert self.commit_index >= 0
            assert self.last_applied <= self.commit_index
            assert self.last_applied <= len(self.log)
            assert self.commit_index <= len(self.log)
            
            for i in range(1, len(self.log)):
                # Monotonic terms in log
                assert self.log[i].term >= self.log[i-1].term

    # --- State Transitions & Internal Logic ---
    def convert_to_follower(self, term: int):
        self.current_term = term
        self.state = "Follower"
        self.voted_for = None
        self.votes_received.clear()

    def start_election(self):
        with self.lock:
            if self.state == "Leader":
                return
            self.state = "Candidate"
            self.current_term += 1
            self.voted_for = self.node_id
            self.votes_received = {self.node_id}
            
            term = self.current_term
            last_log_index = len(self.log)
            last_log_term = self.log[-1].term if last_log_index > 0 else 0

        # Broadcast RequestVote
        for peer_id in self.peer_ids:
            peer = self.all_nodes.get(peer_id)
            if peer:
                payload = {
                    "term": term,
                    "candidate_id": self.node_id,
                    "last_log_index": last_log_index,
                    "last_log_term": last_log_term
                }
                peer.deliver_message("REQUEST_VOTE", payload)

    def _handle_request_vote(self, payload: Dict[str, Any]):
        term = payload["term"]
        candidate_id = payload["candidate_id"]
        last_log_index = payload["last_log_index"]
        last_log_term = payload["last_log_term"]

        grant = False

        if term > self.current_term:
            self.convert_to_follower(term)

        if term == self.current_term and (self.voted_for is None or self.voted_for == candidate_id):
            # Check log up-to-date
            self_last_index = len(self.log)
            self_last_term = self.log[-1].term if self_last_index > 0 else 0

            up_to_date = False
            if last_log_term > self_last_term:
                up_to_date = True
            elif last_log_term == self_last_term and last_log_index >= self_last_index:
                up_to_date = True

            if up_to_date:
                self.voted_for = candidate_id
                grant = True

        peer = self.all_nodes.get(candidate_id)
        if peer:
            peer.deliver_message("REQUEST_VOTE_RESP", {
                "term": self.current_term,
                "vote_granted": grant,
                "from_node": self.node_id
            })

    def _handle_request_vote_resp(self, payload: Dict[str, Any]):
        term = payload["term"]
        vote_granted = payload["vote_granted"]
        from_node = payload["from_node"]

        if term > self.current_term:
            self.convert_to_follower(term)
            return

        if self.state == "Candidate" and term == self.current_term and vote_granted:
            self.votes_received.add(from_node)
            total_nodes = len(self.peer_ids) + 1
            if len(self.votes_received) > total_nodes // 2:
                self.state = "Leader"
                last_index = len(self.log)
                for pid in self.peer_ids:
                    self.next_index[pid] = last_index + 1
                    self.match_index[pid] = 0
                self._send_append_entries_to_all()

    def _send_append_entries_to_all(self):
        for peer_id in self.peer_ids:
            self._send_append_entries(peer_id)

    def _send_append_entries(self, peer_id: int):
        peer = self.all_nodes.get(peer_id)
        if not peer:
            return

        prev_log_index = self.next_index.get(peer_id, 1) - 1
        prev_log_term = 0
        if prev_log_index > 0 and prev_log_index <= len(self.log):
            prev_log_term = self.log[prev_log_index - 1].term

        entries = []
        if len(self.log) >= prev_log_index + 1:
            entries = [LogEntry(e.term, e.index, e.command) for e in self.log[prev_log_index:]]

        payload = {
            "term": self.current_term,
            "leader_id": self.node_id,
            "prev_log_index": prev_log_index,
            "prev_log_term": prev_log_term,
            "entries": entries,
            "leader_commit": self.commit_index
        }
        peer.deliver_message("APPEND_ENTRIES", payload)

    def _handle_append_entries(self, payload: Dict[str, Any]):
        term = payload["term"]
        leader_id = payload["leader_id"]
        prev_log_index = payload["prev_log_index"]
        prev_log_term = payload["prev_log_term"]
        entries = payload["entries"]
        leader_commit = payload["leader_commit"]

        success = False

        if term > self.current_term:
            self.convert_to_follower(term)

        if term == self.current_term:
            if self.state == "Candidate":
                self.state = "Follower"
            
            # 1. Reply false if term < currentTerm
            # 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
            if prev_log_index == 0 or (
                prev_log_index <= len(self.log) and self.log[prev_log_index - 1].term == prev_log_term
            ):
                success = True
                
                # 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
                # 4. Append any new entries not already in the log
                insert_idx = prev_log_index
                for entry in entries:
                    insert_idx += 1
                    if insert_idx <= len(self.log):
                        if self.log[insert_idx - 1].term != entry.term:
                            # Conflict: truncate
                            self.log = self.log[:insert_idx - 1]
                            self.log.append(entry)
                    else:
                        self.log.append(entry)

                # 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
                if leader_commit > self.commit_index:
                    self.commit_index = min(leader_commit, len(self.log))
                    self._apply_entries()

        peer = self.all_nodes.get(leader_id)
        if peer:
            peer.deliver_message("APPEND_ENTRIES_RESP", {
                "term": self.current_term,
                "success": success,
                "match_index": len(self.log) if success else prev_log_index,
                "from_node": self