import asyncio
import binascii
import dataclasses
import enum
import logging
import mmap
import os
import struct
import time
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("raft.engine")


class RaftError(Exception):
    """Base exception for Raft engine errors."""
    pass


class LogCorruptionError(RaftError):
    """Raised when WAL log corruption or invalid CRC32C is detected."""
    pass


class SnapshotError(RaftError):
    """Raised when state machine snapshot serialization or deserialization fails."""
    pass


class NodeState(enum.Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


@dataclasses.dataclass(frozen=True)
class LogEntry:
    term: int
    index: int
    opcode: bytes
    checksum: int = 0

    def serialize(self) -> bytes:
        # Format: term (Q), index (Q), payload_len (I), opcode (variable), checksum (I)
        # We compute checksum over term, index, and opcode.
        payload_len = len(self.opcode)
        header = struct.pack(">QQI", self.term, self.index, payload_len)
        body = header + self.opcode
        chk = binascii.crc32(body) & 0xFFFFFFFF
        return body + struct.pack(">I", chk)

    @classmethod
    def deserialize(cls, data: bytes) -> Tuple["LogEntry", int]:
        if len(data) < 24:
            raise LogCorruptionError("Insufficient data for log entry header and checksum.")
        term, index, payload_len = struct.unpack(">QQI", data[0:20])
        total_size = 20 + payload_len + 4
        if len(data) < total_size:
            raise LogCorruptionError(f"Insufficient data for payload. Expected {total_size}, got {len(data)}.")
        opcode = data[20 : 20 + payload_len]
        stored_chk = struct.unpack(">I", data[20 + payload_len : total_size])[0]

        body = data[0 : 20 + payload_len]
        computed_chk = binascii.crc32(body) & 0xFFFFFFFF
        if computed_chk != stored_chk:
            raise LogCorruptionError(f"CRC32C mismatch: computed {computed_chk:#x}, stored {stored_chk:#x}")

        entry = cls(term=term, index=index, opcode=opcode, checksum=stored_chk)
        return entry, total_size


class PersistentState:
    def __init__(self, wal_path: str):
        self.wal_path = wal_path
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.last_included_index: int = 0
        self.last_included_term: int = 0
        self._file = None
        self._lock = asyncio.Lock()
        self._init_storage()

    def _init_storage(self) -> None:
        dir_name = os.path.dirname(self.wal_path)
        if dir_name and not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)
        
        if not os.path.exists(self.wal_path):
            with open(self.wal_path, "wb") as f:
                # Write initial metadata header: term(Q), voted_for_len(H), voted_for(str), last_inc_idx(Q), last_inc_term(Q)
                meta = struct.pack(">QHQQ", 0, 0, 0, 0)
                f.write(meta)
                f.flush()
                os.fsync(f.fileno())
        
        self._file = open(self.wal_path, "r+b")
        self._recover_and_load_metadata()

    def _recover_and_load_metadata(self) -> None:
        self._file.seek(0)
        header_data = self._file.read(26)
        if len(header_data) < 26:
            raise LogCorruptionError("Persistent state file truncated at metadata header.")
        
        term, vlen, last_inc_idx, last_inc_term = struct.unpack(">QHQQ", header_data[:26])
        self.current_term = term
        if vlen > 0:
            voted_bytes = self._file.read(vlen)
            if len(voted_bytes) < vlen:
                raise LogCorruptionError("Truncated voted_for string in persistent state.")
            self.voted_for = voted_bytes.decode("utf-8")
        else:
            self.voted_for = None
        
        self.last_included_index = last_inc_idx
        self.last_included_term = last_inc_term

    async def update_state(self, term: int, voted_for: Optional[str]) -> None:
        async with self._lock:
            self.current_term = term
            self.voted_for = voted_for
            await asyncio.to_thread(self._sync_metadata_sync)

    def _sync_metadata_sync(self) -> None:
        self._file.seek(0)
        voted_bytes = self.voted_for.encode("utf-8") if self.voted_for else b""
        vlen = len(voted_bytes)
        header = struct.pack(">QHQQ", self.current_term, vlen, self.last_included_index, self.last_included_term)
        self._file.write(header + voted_bytes)
        self._file.flush()
        os.fsync(self._file.fileno())

    async def append_log_entries(self, entries: List[LogEntry]) -> None:
        async with self._lock:
            await asyncio.to_thread(self._append_log_entries_sync, entries)

    def _append_log_entries_sync(self, entries: List[LogEntry]) -> None:
        # Seek past metadata block. To keep it simple, we scan/seek to end of log or maintain offset.
        # For durability, we append to the end of the file.
        self._file.seek(0, os.SEEK_END)
        for entry in entries:
            data = entry.serialize()
            self._file.write(data)
        self._file.flush()
        os.fsync(self._file.fileno())

    async def load_logs(self) -> List[LogEntry]:
        async with self._lock:
            return await asyncio.to_thread(self._load_logs_sync)

    def _load_logs_sync(self) -> List[LogEntry]:
        self._file.seek(0)
        # Skip metadata header
        header_data = self._file.read(26)
        if len(header_data) < 26:
            return []
        _, vlen, _, _ = struct.unpack(">QHQQ", header_data[:26])
        self._file.seek(26 + vlen)

        entries: List[LogEntry] = []
        while True:
            pos = self._file.tell()
            len_bytes = self._file.read(20)
            if len(len_bytes) < 20:
                break
            _, _, payload_len = struct.unpack(">QQI", len_bytes)
            total_size = 20 + payload_len + 4
            self._file.seek(pos)
            entry_bytes = self._file.read(total_size)
            if len(entry_bytes) < total_size:
                # Truncated trailing block detected; truncate file here and break
                logger.warning(f"Truncated log entry detected at byte offset {pos}. Pruning.")
                self._file.seek(pos)
                self._file.truncate()
                break
            try:
                entry, _ = LogEntry.deserialize(entry_bytes)
                entries.append(entry)
            except LogCorruptionError as e:
                logger.error(f"Corruption detected during WAL replay at offset {pos}: {e}. Pruning trailing logs.")
                self._file.seek(pos)
                self._file.truncate()
                break
        return entries

    def close(self) -> None:
        if self._file:
            self._file.close()


class VolatileState:
    def __init__(self, peer_ids: List[str]):
        self.commit_index: int = 0
        self.last_applied: int = 0
        self.next_index: Dict[str, int] = {peer: 1 for peer in peer_ids}
        self.match_index: Dict[str, int] = {peer: 0 for peer in peer_ids}
        self._lock = asyncio.Lock()

    async def get_commit_index(self) -> int:
        async with self._lock:
            return self.commit_index

    async def set_commit_index(self, val: int) -> None:
        async with self._lock:
            self.commit_index = val

    async def get_last_applied(self) -> int:
        async with self._lock:
            return self.last_applied

    async def set_last_applied(self, val: int) -> None:
        async with self._lock:
            self.last_applied = val

    async def get_peer_indices(self, peer: str) -> Tuple[int, int]:
        async with self._lock:
            return self.next_index.get(peer, 1), self.match_index.get(peer, 0)

    async def set_peer_indices(self, peer: str, next_idx: int, match_idx: int) -> None:
        async with self._lock:
            self.next_index[peer] = next_idx
            self.match_index[peer] = match_idx


class StateMachine:
    async def apply(self, opcode: bytes) -> bytes:
        raise NotImplementedError

    async def snapshot(self) -> bytes:
        raise NotImplementedError

    async def restore(self, snapshot_data: bytes) -> None:
        raise NotImplementedError


class KVStateMachine(StateMachine):
    def __init__(self, snapshot_path: str):
        self.store: Dict[str, bytes] = {}
        self.snapshot_path = snapshot_path
        self._lock = asyncio.Lock()

    async def apply(self, opcode: bytes) -> bytes:
        async with self._lock:
            # Opcode format: cmd_type(1B), key_len(H), key, val_len(I), val
            if len(opcode) < 3:
                raise RaftError("Invalid opcode length")
            cmd_type = opcode[0]
            if cmd_type == 1:  # SET
                key_len = struct.unpack(">H", opcode[1:3])[0]
                key = opcode[3 : 3 + key_len].decode("utf-8")
                val_len = struct.unpack(">I", opcode[3 + key_len : 7 + key_len])[0]
                val = opcode[7 + key_len : 7 + key_len + val_len]
                self.store[key] = val
                return b"OK"
            elif cmd_type == 2:  # DELETE
                key_len = struct.unpack(">H", opcode[1:3])[0]
                key = opcode[3 : 3 + key_len].decode("utf-8")
                self.store.pop(key, None)
                return b"OK"
            else:
                raise RaftError(f"Unknown opcode command type: {cmd_type}")

    async def snapshot(self) -> bytes:
        async with self._lock:
            import pickle
            data = pickle.dumps(self.store)
            if self.snapshot_path:
                dir_name = os.path.dirname(self.snapshot_path)
                if dir_name and not os.path.exists(dir_name):
                    os.makedirs(dir_name, exist_ok=True)
                # mmap backed zero-copy snapshot write
                with open(self.snapshot_path, "wb") as f:
                    f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
            return data

    async def restore(self, snapshot_data: bytes) -> None:
        async with self._lock:
            import pickle
            self.store = pickle.loads(snapshot_data)


class RaftEngine:
    def __init__(self, node_id: str, peers: List[str], wal_path: str, snapshot_path: str, state_machine: StateMachine):
        self.node_id = node_id
        self.peers = peers
        self.state = NodeState.FOLLOWER
        self.persistent = PersistentState(wal_path)
        self.volatile = VolatileState(peers)
        self.state_machine = state_machine
        self.snapshot_path = snapshot_path
        
        self.log: List[LogEntry] = []
        self.current_leader: Optional[str] = None
        
        self._running = False
        self._election_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._apply_task: Optional[asyncio.Task] = None
        
        self._votes_received = set()
        self._last_heartbeat = time.monotonic()
        self._election_timeout = self._random_election_timeout()
        self.engine_lock = asyncio.Lock()

    def _random_election_timeout(self) -> float:
        import random
        return random.uniform(0.150, 0.300)

    async def start(self) -> None:
        async with self.engine_lock:
            loaded_logs = await self.persistent.load_logs()
            self.log = loaded_logs
            if self.log:
                last_idx = self.log[-1].index
                await self.volatile.set_commit_index(min(self.volatile.commit_index, last_idx))
                await self.volatile.set_last_applied(min(self.volatile.last_applied, last_idx))

            self._running = True
            self._election_task = asyncio.create_task(self._election_loop())
            self._apply_task = asyncio.create_task(self._apply_loop())
            logger.info(f"RaftEngine started for node {self.node_id} in term {self.persistent.current_term}")

    async def stop(self) -> None:
        async with self.engine_lock:
            self._running = False
            for task in [self._election_task, self._heartbeat_task, self._apply_task]:
                if task:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            self.persistent.close()
            logger.info(f"RaftEngine stopped for node {self.node_id}")

    async def _election_loop(self) -> None:
        while self._running:
            await asyncio.sleep(0.010)
            async with self.engine_lock:
                if self.state == NodeState.LEADER:
                    continue
                now = time.monotonic()
                if now - self._last_heartbeat > self._election_timeout:
                    await self._start_election()

    async def _start_election(self) -> None:
        self.state = NodeState.CANDIDATE
        new_term = self.persistent.current_term + 1
        await self.persistent.update_state(new_term, self.node_id)
        self._votes_received = {self.node_id}
        self._last_heartbeat = time.monotonic()
        self._election_timeout = self._random_election_timeout()
        logger.info(f"Node {self.node_id} starting election for term {new_term}")

        last_log_term = self.log[-1].term if self.log else 0
        last_log_index = self.log[-1].index if self.log else 0

        # Broadcast RequestVote RPC to peers concurrently
        for peer in self.peers:
            asyncio.create_task(self._send_request_vote(peer, new_term, last_log_index, last_log_term))

    async def _send_request_vote(self, peer: str, term: int, last_log_index: int, last_log_term: int) -> None: