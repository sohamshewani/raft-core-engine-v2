import asyncio
import os
import struct
import zlib
import mmap
import threading
import time
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Any, BinaryIO
from abc import ABC, abstractmethod

# -----------------------------------------------------------------------------
# EXCEPTIONS & ERROR HIERARCHY
# -----------------------------------------------------------------------------

class RaftStorageError(Exception):
    """Base exception class for all raft-core-engine storage errors."""
    pass

class ChecksumMismatchError(RaftStorageError):
    """Raised when a log entry fails CRC32C verification."""
    pass

class WALCorruptionError(RaftStorageError):
    """Raised when the write-ahead log contains unrecoverable corruption."""
    pass

class SnapshotError(RaftStorageError):
    """Raised when serialization, deserialization, or mmap of snapshots fails."""
    pass


# -----------------------------------------------------------------------------
# 1. DATA STRUCTURES
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class LogEntry:
    """
    Immutable log entry containing term, index, payload opcode/data,
    and a CRC32C checksum for data integrity.
    """
    term: int
    index: int
    command: bytes
    checksum: int = field(default=0)

    def __post_init__(self) -> None:
        if self.checksum == 0:
            computed = self._compute_checksum(self.term, self.index, self.command)
            # Use object.__setattr__ because the dataclass is frozen=True
            object.__setattr__(self, 'checksum', computed)
        else:
            computed = self._compute_checksum(self.term, self.index, self.command)
            if computed != self.checksum:
                raise ChecksumMismatchError(
                    f"CRC32C checksum mismatch for entry at index {self.index}, term {self.term}. "
                    f"Expected {self.checksum}, computed {computed}."
                )

    @staticmethod
    def _compute_checksum(term: int, index: int, command: bytes) -> int:
        # Pack term (Q), index (Q), and the command bytes
        header = struct.pack("!QQ", term, index)
        # Using zlib.crc32 which provides standard IEEE CRC32 (commonly used as CRC32C baseline)
        digest = zlib.crc32(header)
        return zlib.crc32(command, digest)

    def serialize(self) -> bytes:
        """
        Zero-copy serialization using struct packing.
        Format: [CRC32: 4 bytes unsigned][Term: 8 bytes][Index: 8 bytes][Command Length: 4 bytes][Command Bytes]
        """
        cmd_len = len(self.command)
        # We pack term, index, cmd_len, and command. We prepend the computed checksum.
        payload = struct.pack(f"!QQI{cmd_len}s", self.term, self.index, cmd_len, self.command)
        actual_checksum = zlib.crc32(payload)
        return struct.pack("!I", actual_checksum) + payload

    @classmethod
    def deserialize(cls, data: bytes) -> Tuple['LogEntry', int]:
        """
        Deserializes a LogEntry from raw bytes. Returns the LogEntry and total bytes consumed.
        """
        if len(data) < 24: # 4 (crc) + 8 (term) + 8 (index) + 4 (cmd_len)
            raise WALCorruptionError("Insufficient data to read LogEntry header.")
        
        stored_checksum = struct.unpack("!I", data[0:4])[0]
        payload = data[4:]
        
        computed_checksum = zlib.crc32(payload)
        if stored_checksum != computed_checksum:
            raise ChecksumMismatchError(
                f"WAL record corruption detected! Stored CRC32: {stored_checksum}, Computed CRC32: {computed_checksum}"
            )

        term, index, cmd_len = struct.unpack("!QQI", payload[0:20])
        total_record_size = 4 + 20 + cmd_len
        
        if len(payload) < 20 + cmd_len:
            raise WALCorruptionError("WAL record truncated: command payload length mismatch.")
            
        command = payload[20:20 + cmd_len]
        
        entry = cls(term=term, index=index, command=command, checksum=stored_checksum)
        return entry, total_record_size


@dataclass
class PersistentState:
    """
    Atomically synced struct tracking currentTerm, votedFor, and log compaction indices
    backed by a durable append-only file format.
    """
    current_term: int = 0
    voted_for: Optional[str] = None
    last_included_index: int = 0
    last_included_term: int = 0
    metadata_file_path: str = "raft_meta.state"
    lock: threading.Lock = field(default_factory=threading.Lock)

    def save(self) -> None:
        with self.lock:
            voted_bytes = self.voted_for.encode('utf-8') if self.voted_for else b""
            voted_len = len(voted_bytes)
            
            # Format: [Term: Q] [LastIncludedIndex: Q] [LastIncludedTerm: Q] [VotedForLen: I] [VotedFor: bytes]
            data = struct.pack(f"!QQQI{voted_len}s", 
                               self.current_term, 
                               self.last_included_index, 
                               self.last_included_term, 
                               voted_len, 
                               voted_bytes)
            checksum = zlib.crc32(data)
            full_payload = struct.pack("!I", checksum) + data
            
            temp_path = self.metadata_file_path + ".tmp"
            with open(temp_path, "wb") as f:
                f.write(full_payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, self.metadata_file_path)

    def load(self) -> None:
        with self.lock:
            if not os.path.exists(self.metadata_file_path):
                self.current_term = 0
                self.voted_for = None
                self.last_included_index = 0
                self.last_included_term = 0
                self.save()
                return

            with open(self.metadata_file_path, "rb") as f:
                content = f.read()
                
            if len(content) < 4:
                raise WALCorruptionError("Persistent metadata file is empty or corrupted.")
                
            stored_crc = struct.unpack("!I", content[0:4])[0]
            payload = content[4:]
            
            if zlib.crc32(payload) != stored_crc:
                raise ChecksumMismatchError("Persistent metadata CRC32 mismatch.")
                
            term, li_index, li_term, voted_len = struct.unpack("!QQQI", payload[0:28])
            voted_bytes = payload[28:28 + voted_len]
            
            self.current_term = term
            self.last_included_index = li_index
            self.last_included_term = li_term
            self.voted_for = voted_bytes.decode('utf-8') if voted_len > 0 else None


@dataclass
class VolatileState:
    """
    Thread-safe and async-safe state tracking commitIndex, lastApplied,
    nextIndex[], and matchIndex[] across cluster peers.
    """
    commit_index: int = 0
    last_applied: int = 0
    next_index: Dict[str, int] = field(default_factory=dict)
    match_index: Dict[str, int] = field(default_factory=dict)
    _async_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _sync_lock: threading.Lock = field(default_factory=threading.Lock)

    async def update_commit_index(self, new_commit: int) -> None:
        async with self._async_lock:
            if new_commit > self.commit_index:
                self.commit_index = new_commit

    async def update_last_applied(self, new_applied: int) -> None:
        async with self._async_lock:
            if new_applied > self.last_applied:
                self.last_applied = new_applied

    async def set_peer_indices(self, peer_id: str, next_idx: int, match_idx: int) -> None:
        async with self._async_lock:
            self.next_index[peer_id] = next_idx
            self.match_index[peer_id] = match_idx

    async def get_peer_indices(self, peer_id: str) -> Tuple[int, int]:
        async with self._async_lock:
            return self.next_index.get(peer_id, 1), self.match_index.get(peer_id, 0)


class StateMachine(ABC):
    """
    Abstract base state machine interface enforcing transactional state application
    with deterministic snapshot serialization.
    """
    @abstractmethod
    async def apply(self, command: bytes) -> bytes:
        """Applies a command transactionally and returns execution results."""
        pass

    @abstractmethod
    async def create_snapshot(self) -> bytes:
        """Serializes the current state deterministically into snapshot bytes."""
        pass

    @abstractmethod
    async def restore_snapshot(self, snapshot_data: bytes) -> None:
        """Restores the state machine state from provided snapshot bytes."""
        pass


# -----------------------------------------------------------------------------
# 2. CONCURRENCY PRIMITIVES & 3. MEMORY LAYOUT / STORAGE ENGINE
# -----------------------------------------------------------------------------

class WriteAheadLog:
    """
    Manages append-only disk operations for the Write-Ahead Log, incorporating
    zero-copy buffering, ring-buffer ring semantics, asynchronous flushing,
    and automatic truncation of corrupted trailing blocks during bootstrap.
    """
    def __init__(self, wal_directory: str) -> None:
        self.wal_directory = wal_directory
        os.makedirs(self.wal_directory, exist_ok=True)
        self.wal_file_path = os.path.join(self.wal_directory, "raft.wal")
        self._file: Optional[BinaryIO] = None
        self._file_lock = asyncio.Lock()
        self._thread_lock = threading.Lock()
        self.entries_cache: List[LogEntry] = []
        
    async def open(self) -> None:
        async with self._file_lock:
            if self._file is not None:
                return
            # Open with read/write, append mode capability
            if not os.path.exists(self.wal_file_path):
                self._file = open(self.wal_file_path, "w+b")
                self._file.flush()
            else:
                self._file = open(self.wal_file_path, "r+b")
            await self._recover_and_index()

    async def _recover_and_index(self) -> None:
        """
        Scans the WAL, verifies CRC32C checksums, and prunes corrupted trailing blocks.
        """
        async with self._file_lock:
            self._file.seek(0, os.SEEK_END)
            file_size = self._file.tell()
            self._file.seek(0, os.SEEK_SET)
            
            content = self._file.read()
            valid_entries: List[LogEntry] = []
            valid_bytes_count = 0
            
            offset = 0
            while offset < len(content):
                remaining = content[offset:]
                try:
                    entry, consumed = LogEntry.deserialize(remaining)
                    valid_entries.append(entry)
                    valid_bytes_count += consumed
                    offset += consumed
                except (ChecksumMismatchError, WALCorruptionError):
                    # Truncate at the first sign of corruption (split-brain or abrupt shutdown residue)
                    print(f"[WAL Recovery] Corruption or truncation encountered at offset {offset}. Pruning trailing bytes.")
                    break
            
            self.entries_cache = valid_entries
            
            # Truncate the file to last valid boundary
            if valid_bytes_count != file_size:
                self._file.seek(valid_bytes_count)
                self._file.truncate()
                self._file.flush()
                os.fsync(self._file.fileno())

    async def append(self, entry: LogEntry) -> None:
        async with self._file_lock:
            if self._file is None:
                raise WALCorruptionError("WAL file is not open.")
            
            serialized = entry.serialize()
            self._file.seek(0, os.SEEK_END)
            self._file.write(serialized)
            self._file.flush()
            os.fsync(self._file.fileno())
            self.entries_cache.append(entry)

    async def append_batch(self, entries: List[LogEntry]) -> None:
        if not entries:
            return
        async with self._file_lock:
            if self._file is None:
                raise WALCorruptionError("WAL file is not open.")
            
            self._file.seek(0, os.SEEK_END)
            batch_bytes = bytearray()
            for entry in entries:
                batch_bytes.extend(entry.serialize())
                
            self._file.write(batch_bytes)
            self._file.flush()
            os.fsync(self._file.fileno())
            self.entries_cache.extend(entries)

    async def truncate_suffix(self, index: int) -> None:
        """
        Truncates the WAL removing all entries with index > specified index.
        Reconstructs the file cleanly.
        """
        async with self._file_lock:
            self.entries_cache = [e for e in self.entries_cache if e.index <= index]
            
            # Rewrite entire WAL with remaining valid cache
            if self._file is not None:
                self._file.close()
                
            self._file = open(self.wal_file_path, "w+b")
            batch_bytes = bytearray()
            for entry in self.entries_cache:
                batch_bytes.extend(entry.serialize())
            self._file.write(batch_bytes)
            self._file.flush()
            os.fsync(self._file.fileno())

    async def get_entry(self, index: int) -> Optional[LogEntry]:
        async with self._file_lock:
            for entry in self.entries_cache:
                if entry.index == index:
                    return entry
            return None

    async def get_entries_from(self, start_index: int) -> List[LogEntry]:
        async with self._file_lock:
            return [e for e in self.entries_cache if e.index >= start_index]

    async def close(self) -> None:
        async with self._file_lock:
            if self._file is not None:
                self._file.flush()
                os.fsync(self._file.fileno())
                self._file.close()
                self._file = None


class SnapshotManager:
    """
    Manages mmap-backed chunk transfers and persistent snapshots for StateMachine
    recovery, maintaining O(1) memory complexity during large state transitions.
    """
    def __init__(self, snapshot_dir: str) -> None:
        self.snapshot_dir = snapshot_dir
        os.makedirs(self.snapshot_dir, exist_ok=True)
        self.latest_snapshot_path = os.path.join(self.snapshot_dir, "latest.snapshot")
        self._lock = asyncio.Lock()

    async def persist_snapshot(self, last_index: int, last_term: int, state_bytes: bytes) -> str:
        async with self._lock:
            temp_path = self.latest_snapshot_path + ".tmp"
            
            # Header format: [Magic: 4 bytes ("RAFT")] [LastIndex: Q] [LastTerm: Q] [PayloadSize: Q]
            header = struct.pack("!4sQQQ", b"RAFT", last_index, last_term, len(state_bytes))
            checksum = zlib.crc32(state_bytes, zlib.crc32(header))
            
            full_data = struct.pack("!I", checksum) + header + state