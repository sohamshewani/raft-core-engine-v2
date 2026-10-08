import asyncio
import collections
import contextlib
import dataclasses
import enum
import logging
import mmap
import os
import struct
import threading
import time
import zlib
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple, Union

logger = logging.getLogger("raft.concurrency")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s"))
    logger.addHandler(handler)


class RaftError(Exception):
    """Base exception class for all Raft-core-engine errors."""
    pass


class ChecksumError(RaftError):
    """Raised when a log entry or block fails CRC32C checksum verification."""
    pass


class DiskCorruptionError(RaftError):
    """Raised when persistent state or WAL encounters irrecoverable structural corruption."""
    pass


class ElectionTimeoutError(RaftError):
    """Raised when election timer expires without leader contact."""
    pass


class StateMachineError(RaftError):
    """Raised when state machine application or snapshotting fails."""
    pass


class NodeRole(enum.Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


@dataclasses.dataclass(frozen=True)
class LogEntry:
    term: int
    index: int
    opcode: int
    payload: bytes
    checksum: int = dataclasses.field(default=0)

    def __post_init__(self) -> bool:
        if self.checksum == 0:
            computed = self.compute_checksum(self.term, self.index, self.opcode, self.payload)
            object.__setattr__(self, 'checksum', computed)
        else:
            expected = self.compute_checksum(self.term, self.index, self.opcode, self.payload)
            if self.checksum != expected:
                raise ChecksumError(
                    f"CRC32C checksum mismatch for LogEntry at index {self.index}, term {self.term}: "
                    f"expected {expected:#x}, got {self.checksum:#x}"
                )
        return True

    @staticmethod
    def compute_checksum(term: int, index: int, opcode: int, payload: bytes) -> int:
        header = struct.pack("!Q Q I", term, index, opcode)
        hasher = zlib.crc32(header)
        hasher = zlib.crc32(payload, hasher)
        return hasher & 0xFFFFFFFF

    def serialize(self) -> bytes:
        header = struct.pack("!Q Q I I", self.term, self.index, self.opcode, len(self.payload))
        return header + self.payload + struct.pack("!I", self.checksum)

    @classmethod
    def deserialize(cls, buffer: memoryview) -> Tuple['LogEntry', int]:
        HEADER_FORMAT = "!QQII"
        HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
        if len(buffer) < HEADER_SIZE:
            raise DiskCorruptionError("Buffer too small to deserialize LogEntry header.")
        
        term, index, opcode, payload_len = struct.unpack_from(HEADER_FORMAT, buffer, 0)
        total_size = HEADER_SIZE + payload_len + 4
        if len(buffer) < total_size:
            raise DiskCorruptionError(f"Buffer truncated: expected {total_size} bytes, got {len(buffer)} bytes.")
        
        payload_bytes = bytes(buffer[HEADER_SIZE : HEADER_SIZE + payload_len])
        checksum_offset = HEADER_SIZE + payload_len
        stored_checksum, = struct.unpack_from("!I", buffer, checksum_offset)
        
        entry = cls(
            term=term,
            index=index,
            opcode=opcode,
            payload=payload_bytes,
            checksum=stored_checksum
        )
        return entry, total_size


class PersistentState:
    def __init__(self, wal_path: str) -> None:
        self.wal_path = wal_path
        self._lock = threading.RLock()
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.last_included_index: int = 0
        self.last_included_term: int = 0
        self._file: Optional[BinaryIO] = None
        self._mmap_obj: Optional[mmap.mmap] = None
        self._initialize_storage()

    def _initialize_storage(self) -> None:
        with self._lock:
            dir_name = os.path.dirname(self.wal_path)
            if dir_name and not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
            
            meta_path = f"{self.wal_path}.meta"
            if os.path.exists(meta_path):
                with open(meta_path, "rb") as f:
                    data = f.read()
                    if len(data) >= 24:
                        self.current_term, last_voted_len, self.last_included_index, self.last_included_term = struct.unpack("!Q Q Q Q", data[:32])
                        voted_bytes = data[32:32+last_voted_len]
                        self.voted_for = voted_bytes.decode('utf-8') if voted_bytes else None
            
            if not os.path.exists(self.wal_path):
                with open(self.wal_path, "wb") as f:
                    f.flush()
            
            self._file = open(self.wal_path, "a+b")
            self._sync_mmap()

    def _sync_mmap(self) -> None:
        if self._mmap_obj is not None:
            self._mmap_obj.close()
            self._mmap_obj = None
        
        fileno = self._file.fileno()
        file_size = os.fstat(fileno).st_size
        if file_size > 0:
            self._mmap_obj = mmap.mmap(fileno, 0, access=mmap.ACCESS_READ)
        else:
            self._mmap_obj = None

    def flush_metadata(self) -> None:
        with self._lock:
            meta_path = f"{self.wal_path}.meta"
            voted_bytes = self.voted_for.encode('utf-8') if self.voted_for else b""
            voted_len = len(voted_bytes)
            header = struct.pack("!Q Q Q Q", self.current_term, voted_len, self.last_included_index, self.last_included_term)
            with open(meta_path, "wb") as f:
                f.write(header + voted_bytes)
                f.flush()
                os.fsync(f.fileno())

    def set_term_and_vote(self, term: int, voted_for: Optional[str]) -> None:
        with self._lock:
            self.current_term = term
            self.voted_for = voted_for
            self.flush_metadata()

    def append_entry(self, entry: LogEntry) -> None:
        with self._lock:
            data = entry.serialize()
            self._file.seek(0, os.SEEK_END)
            self._file.write(data)
            self._file.flush()
            os.fsync(self._file.fileno())
            self._sync_mmap()

    def recover_log(self) -> List[LogEntry]:
        with self._lock:
            if not self._file:
                return []
            self._file.seek(0, os.SEEK_END)
            file_size = self._file.tell()
            if file_size == 0:
                return []
            
            self._sync_mmap()
            if not self._mmap_obj:
                return []

            entries: List[LogEntry] = []
            offset = 0
            mv = memoryview(self._mmap_obj)
            
            while offset < file_size:
                try:
                    remaining = mv[offset:]
                    entry, bytes_read = LogEntry.deserialize(remaining)
                    entries.append(entry)
                    offset += bytes_read
                except (DiskCorruptionError, ChecksumError) as e:
                    logger.warning(f"WAL corruption detected at offset {offset}: {e}. Truncating trailing blocks.")
                    self._truncate_file(offset)
                    break
            return entries

    def _truncate_file(self, valid_size: int) -> None:
        with self._lock:
            self._file.close()
            with open(self.wal_path, "r+b") as f:
                f.truncate(valid_size)
                f.flush()
                os.fsync(f.fileno())
            self._file = open(self.wal_path, "a+b")
            self._sync_mmap()

    def close(self) -> None:
        with self._lock:
            if self._mmap_obj is not None:
                self._mmap_obj.close()
                self._mmap_obj = None
            if self._file is not None:
                self._file.close()
                self._file = None


class VolatileState:
    def __init__(self, peer_ids: List[str]) -> None:
        self._lock = asyncio.Lock()
        self.commit_index: int = 0
        self.last_applied: int = 0
        self.next_index: Dict[str, int] = {peer: 1 for peer in peer_ids}
        self.match_index: Dict[str, int] = {peer: 0 for peer in peer_ids}

    async def get_commit_index(self) -> int:
        async with self._lock:
            return self.commit_index

    async def set_commit_index(self, val: int) -> None:
        async with self._lock:
            self.commit_index = max(self.commit_index, val)

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

    async def reset_peer_indices(self, peer_ids: List[str], last_log_index: int) -> None:
        async with self._lock:
            for peer in peer_ids:
                self.next_index[peer] = last_log_index + 1
                self.match_index[peer] = 0


class StateMachine:
    async def apply(self, entry: LogEntry) -> bytes:
        raise NotImplementedError

    async def snapshot(self) -> bytes:
        raise NotImplementedError

    async def restore(self, snapshot_data: bytes) -> None:
        raise NotImplementedError


class ReadWriterLock:
    def __init__(self) -> None:
        self._read_lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._readers_count = 0

    @contextlib.contextmanager
    def read_lock(self) -> Iterator[None]:
        with self._read_lock:
            self._readers_count += 1
            if self._readers_count == 1:
                self._write_lock.acquire()
        try:
            yield
        finally:
            with self._read_lock:
                self._readers_count -= 1
                if self._readers_count == 0:
                    self._write_lock.release()

    @contextlib.contextmanager
    def write_lock(self) -> Iterator[None]:
        self._write_lock.acquire()
        try:
            yield
        finally:
            self._write_lock.release()


class ActorModelEventLoop:
    def __init__(self, node_id: str, peer_ids: List[str], state_machine: StateMachine, wal_path: str) -> None:
        self.node_id = node_id
        self.peer_ids = peer_ids
        self.state_machine = state_machine
        self.persistent_state = PersistentState(wal_path)
        self.volatile_state = VolatileState(peer_ids)
        
        self.role = NodeRole.FOLLOWER
        self.leader_id: Optional[str] = None
        self.log: List[LogEntry] = []
        
        self.rw_lock = ReadWriterLock()
        self.disk_executor = asyncio.get_running_loop() if False else None  # initialized in start
        
        self._election_timeout = self._randomize_election_timeout()
        self._heartbeat_interval = 0.05
        self._last_heartbeat_monotonic = time.monotonic()
        
        self._running = False
        self._background_tasks: List[asyncio.Task] = []

    def _randomize_election_timeout(self) -> float:
        import random
        return random.uniform(0.150, 0.300)

    async def start(self) -> None:
        with self.rw_lock.write_lock():
            recovered_entries = self.persistent_state.recover_log()
            self.log.extend(recovered_entries)
            last_idx = self.log[-1].index if self.log else 0
            await self.volatile_state.reset_peer_indices(self.peer_ids, last_idx)
            await self.volatile_state.set_last_applied(self.persistent_state.last_included_index)
            await self.volatile_state.set_commit_index(self.persistent_state.last_included_index)

        self._running = True
        self._last_heartbeat_monotonic = time.monotonic()
        
        self._background_tasks = [
            asyncio.create_task(self._election_ticker_loop()),
            asyncio.create_task(self._apply_entries_loop())
        ]
        logger.info(f"Node {self.node_id} started. Role: {self.role.name}, Term: {self.persistent_state.current_term}")

    async def stop(self) -> None:
        self._running = False
        for task in self._background_tasks:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._background_tasks.clear()
        self.persistent_state.close()
        logger.info(f"Node {self.node_id} stopped cleanly.")

    async def _election_ticker_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(0.01)
                now = time.monotonic()
                
                with self.rw_lock.read_lock():
                    current_role = self.role
                    current_timeout = self._election_timeout
                    last_hb = self._last_heartbeat_monotonic

                if current_role == NodeRole.LEADER:
                    continue

                if (now - last_hb) > current_timeout:
                    logger.warning(f"Node {self.node_id} election timeout expired. Starting election.")
                    await self._start_election()
                    with self.rw_lock.write_lock():
                        self._election_timeout = self._randomize_election_timeout()
                        self._last_heartbeat_monotonic = time.monotonic()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in election ticker loop: {e}", exc_info=True)

    async def _start_election(self) -> None:
        with self.rw_lock.write_lock():
            self.role = NodeRole.CANDIDATE
            self.persistent_state.current_term += 1
            self.persistent_state.voted_for = self.node_id
            self.persistent_state.flush_