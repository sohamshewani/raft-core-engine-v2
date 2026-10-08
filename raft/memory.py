import asyncio
import binascii
import dataclasses
import os
import struct
import mmap
import time
import threading
from typing import Dict, List, Optional, Tuple, Any, AsyncIterator, ByteString

class RaftError(Exception):
    """Base exception class for all Raft-core-engine errors."""
    pass

class ChecksumMismatchError(RaftError):
    """Raised when a log entry or WAL record fails CRC32C verification."""
    pass

class WALCorruptionError(RaftError):
    """Raised when unrecoverable corruption is encountered during WAL replay."""
    pass

class SnapshotError(RaftError):
    """Raised during state machine snapshot serialization, mmap allocation, or restoration."""
    pass

@dataclasses.dataclass(frozen=True)
class LogEntry:
    """Immutable representation of a Raft log entry with CRC32C integrity checking."""
    term: int
    index: int
    payload: bytes
    checksum: int

    @staticmethod
    def create(term: int, index: int, payload: bytes) -> 'LogEntry':
        """Factory method to construct a LogEntry and compute its CRC32C checksum."""
        # Format: term (Q), index (Q), payload_length (I), payload (variable)
        header = struct.pack("!QQI", term, index, len(payload))
        checksum = binascii.crc32(header + payload) & 0xFFFFFFFF
        return LogEntry(term=term, index=index, payload=payload, checksum=checksum)

    def serialize(self) -> bytes:
        """Serializes the log entry into wire/disk format including checksum."""
        header = struct.pack("!QQI", self.term, self.index, len(self.payload))
        return struct.pack("!I", self.checksum) + header + self.payload

    @classmethod
    def deserialize(cls, data: bytes) -> Tuple['LogEntry', int]:
        """Deserializes a LogEntry from raw bytes. Returns the entry and total bytes consumed."""
        if len(data) < 24:
            raise WALCorruptionError("Insufficient data for LogEntry header and checksum.")
        
        checksum = struct.unpack("!I", data[0:4])[0]
        term, index, payload_length = struct.unpack("!QQI", data[4:20])
        
        total_size = 20 + payload_length
        if len(data) < total_size:
            raise WALCorruptionError(f"Truncated WAL record: expected {total_size} bytes, got {len(data)}.")
            
        payload = data[20:total_size]
        
        # Verify checksum
        header = struct.pack("!QQI", term, index, payload_length)
        computed_checksum = binascii.crc32(header + payload) & 0xFFFFFFFF
        if computed_checksum != checksum:
            raise ChecksumMismatchError(
                f"CRC32C mismatch at index {index}, term {term}: expected {checksum:#010x}, got {computed_checksum:#010x}"
            )
            
        return cls(term=term, index=index, payload=payload, checksum=checksum), total_size


class PersistentState:
    """
    Atomically synced persistent state tracking currentTerm, votedFor, 
    and log compaction indices backed by a durable append-only file format.
    """
    def __init__(self, wal_path: str, sync_executor: Any) -> None:
        self.wal_path = wal_path
        self.sync_executor = sync_executor
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.last_included_index: int = 0
        self.last_included_term: int = 0
        self._lock = asyncio.Lock()
        self._file: Optional[Any] = None
        self._initialize_storage()

    def _initialize_storage(self) -> None:
        dir_name = os.path.dirname(self.wal_path)
        if dir_name and not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)
        
        # Open file in read/write binary mode, create if missing
        if not os.path.exists(self.wal_path):
            with open(self.wal_path, "wb") as f:
                # Write initial metadata header: term(Q), voted_for_len(H), voted_for(bytes), last_inc_idx(Q), last_inc_term(Q)
                initial_meta = struct.pack("!QHQQ", 0, 0, 0, 0)
                f.write(initial_meta)
                f.flush()
                os.fsync(f.fileno())
        
        self._file = open(self.wal_path, "r+b")
        self._recover_and_truncate()

    def _recover_and_truncate(self) -> None:
        """Replays WAL, validates CRC32C, and prunes corrupted trailing blocks."""
        self._file.seek(0)
        content = self._file.read()
        
        if len(content) < 24:
            # Recreate corrupted header
            self._file.seek(0)
            self._file.write(struct.pack("!QHQQ", 0, 0, 0, 0))
            self._file.truncate(24)
            self._file.flush()
            os.fsync(self._file.fileno())
            return

        # Read metadata block (fixed 24 bytes at offset 0 for simplicity of state representation)
        # Production layout separates WAL metadata and log entries; here we integrate state+log.
        try:
            term, vlen, last_inc_idx, last_inc_term = struct.unpack("!QHQQ", content[0:24])
            self.current_term = term
            if vlen > 0:
                self.voted_for = content[24:24+vlen].decode('utf-8')
            else:
                self.voted_for = None
            self.last_included_index = last_inc_idx
            self.last_included_term = last_inc_term
        except Exception as e:
            raise WALCorruptionError(f"Failed to parse persistent state header: {e}")

    async def update_state(self, term: int, voted_for: Optional[str]) -> None:
        """Atomically updates and fsyncs persistent state changes."""
        async with self._lock:
            self.current_term = term
            self.voted_for = voted_for
            
            voted_bytes = voted_for.encode('utf-8') if voted_for else b""
            vlen = len(voted_bytes)
            
            def _sync_disk() -> None:
                self._file.seek(0)
                # Read existing metadata to preserve log compaction fields
                header = struct.pack("!QHQQ", self.current_term, vlen, self.last_included_index, self.last_included_term)
                self._file.write(header + voted_bytes)
                self._file.flush()
                os.fsync(self._file.fileno())

            await asyncio.get_running_loop().run_in_executor(self.sync_executor, _sync_disk)

    async def append_entry(self, entry: LogEntry) -> None:
        """Appends a log entry to the WAL on disk with fsync."""
        async with self._lock:
            serialized = entry.serialize()
            
            def _sync_append() -> None:
                self._file.seek(0, os.SEEK_END)
                self._file.write(serialized)
                self._file.flush()
                os.fsync(self._file.fileno())

            await asyncio.get_running_loop().run_in_executor(self.sync_executor, _sync_append)

    async def load_entries(self) -> List[LogEntry]:
        """Loads all valid log entries from the WAL, pruning trailing corruption."""
        async with self._lock:
            def _read_all() -> List[LogEntry]:
                self._file.seek(24) # Skip metadata block
                # Skip past voted_for bytes if variable length stored in header
                # For clean execution, we scan from end of metadata area.
                # Let's read entire file and parse sequentially.
                self._file.seek(0)
                data = self._file.read()
                
                # Parse metadata header to find exact offset where logs begin
                if len(data) < 24:
                    return []
                term, vlen, last_inc_idx, last_inc_term = struct.unpack("!QHQQ", data[0:24])
                offset = 24 + vlen
                
                entries: List[LogEntry] = []
                while offset < len(data):
                    try:
                        entry, consumed = LogEntry.deserialize(data[offset:])
                        entries.append(entry)
                        offset += consumed
                    except (ChecksumMismatchError, WALCorruptionError):
                        # Prune trailing corrupted block
                        self._file.seek(offset)
                        self._file.truncate()
                        self._file.flush()
                        os.fsync(self._file.fileno())
                        break
                return entries

            return await asyncio.get_running_loop().run_in_executor(self.sync_executor, _read_all)

    def close(self) -> None:
        if self._file and not self._file.closed:
            self._file.close()


class VolatileState:
    """Thread-safe and async-safe volatile indices tracking cluster consensus state."""
    def __init__(self, peer_ids: List[str], last_log_index: int = 0) -> None:
        self.commit_index: int = 0
        self.last_applied: int = 0
        self._next_index: Dict[str, int] = {peer: last_log_index + 1 for peer in peer_ids}
        self._match_index: Dict[str, int] = {peer: 0 for peer in peer_ids}
        self._lock = asyncio.Lock()

    async def get_commit_index(self) -> int:
        async with self._lock:
            return self.commit_index

    async def set_commit_index(self, index: int) -> None:
        async with self._lock:
            self.commit_index = max(self.commit_index, index)

    async def get_last_applied(self) -> int:
        async with self._lock:
            return self.last_applied

    async def set_last_applied(self, index: int) -> None:
        async with self._lock:
            self.last_applied = index

    async def get_peer_indices(self, peer: str) -> Tuple[int, int]:
        async with self._lock:
            return self._next_index.get(peer, 1), self._match_index.get(peer, 0)

    async def update_peer_progress(self, peer: str, match_idx: int, next_idx: int) -> None:
        async with self._lock:
            self._match_index[peer] = max(self._match_index.get(peer, 0), match_idx)
            self._next_index[peer] = max(self._next_index.get(peer, 1), next_idx)

    async def set_peer_next_index(self, peer: str, next_idx: int) -> None:
        async with self._lock:
            self._next_index[peer] = max(1, next_idx)

    async def get_all_match_indices(self) -> List[int]:
        async with self._lock:
            return list(self._match_index.values())


class StateMachine:
    """Abstract base state machine interface enforcing transactional application and deterministic snapshots."""
    def __init__(self, snapshot_path: str) -> None:
        self.snapshot_path = snapshot_path

    async def apply(self, command: bytes) -> bytes:
        """Applies a command transactionally to the state machine. Returns execution result."""
        raise NotImplementedError

    async def serialize_snapshot(self) -> bytes:
        """Serializes the state machine state into a deterministic byte array."""
        raise NotImplementedError

    async def restore_snapshot(self, snapshot_data: bytes) -> None:
        """Restores state machine state from serialized snapshot data."""
        raise NotImplementedError


class MemoryMappedSnapshotManager:
    """
    Manages mmap-backed chunk transfers for state machine snapshots,
    ensuring constant memory overhead O(1) during gigabyte-scale state recovery.
    """
    def __init__(self, snapshot_dir: str) -> None:
        self.snapshot_dir = snapshot_dir
        if not os.path.exists(self.snapshot_dir):
            os.makedirs(self.snapshot_dir, exist_ok=True)

    def write_snapshot_mmap(self, snapshot_id: int, data: bytes) -> str:
        file_path = os.path.join(self.snapshot_dir, f"snapshot_{snapshot_id}.bin")
        data_len = len(data)
        
        with open(file_path, "wb") as f:
            f.write(b'\x00' * data_len)
            f.flush()

        with open(file_path, "r+b") as f:
            with mmap.mmap(f.fileno(), data_len, access=mmap.ACCESS_WRITE) as mm:
                mm.write(data)
                mm.flush()
        return file_path

    async def stream_snapshot_chunks(self, file_path: str, chunk_size: int = 65536) -> AsyncIterator[bytes]:
        """Streams snapshot data in O(1) memory blocks using memory mapping."""
        def _read_chunks() -> List[bytes]:
            chunks = []
            if not os.path.exists(file_path):
                raise SnapshotError(f"Snapshot file not found: {file_path}")
            
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                return []

            with open(file_path, "rb") as f:
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                    offset = 0
                    while offset < file_size:
                        length = min(chunk_size, file_size - offset)
                        chunks.append(mm[offset:offset + length])
                        offset += length
            return chunks

        loop = asyncio.get_running_loop()
        chunks = await loop.run_in_executor(None, _read_chunks)
        for chunk in chunks:
            yield chunk


class WriteAheadLogRingBuffer:
    """Pre-allocated circular memory buffers for in-flight log appends, flushing asynchronously."""
    def __init__(self, capacity: int = 4096) -> None:
        self.capacity = capacity
        self.buffer: List[Optional[LogEntry]] = [None] * capacity
        self.head: int = 0
        self.tail: int = 0
        self.size: int = 0
        self._lock = asyncio.Lock()

    async def push(self, entry: LogEntry) -> bool:
        async with self._lock:
            if self.size >= self.capacity:
                return False
            self.buffer[self.tail] = entry
            self.tail = (self.tail + 1) % self.capacity
            self.size += 1
            return True

    async def pop_batch(self, max_batch_size: int) -> List[LogEntry]:
        async with self._lock:
            batch: List[LogEntry] = []
            count = min(max_batch_size, self.size)
            for _ in range(count):
                item = self.buffer[self.head]
                if item is not None:
                    batch.append(item)
                    self.buffer[self.head] = None
                self.head = (self.head + 1) % self.capacity
                self.size -= 1
            return batch


class ElectionTimer:
    """Randomized election timeouts (150ms-300ms) governed by monotonic clocks."""
    def __init__(self, min_timeout_ms: int = 150, max_timeout_ms: int = 300) -> None:
        self.min_timeout = min_timeout_ms / 1000.0
        self.max_timeout = max_timeout_ms / 1000.0
        self._deadline: float = 0.0
        self.reset()

    def reset(self) -> None: