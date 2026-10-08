import asyncio
import struct
import zlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class RaftError(Exception):
    """Base exception for all Raft-core-engine errors."""
    pass


class ChecksumError(RaftError):
    """Raised when CRC32C verification fails on a log entry or WAL block."""
    pass


class InvalidStateTransitionError(RaftError):
    """Raised when an illegal state machine or node state transition is attempted."""
    pass


@dataclass(frozen=True)
class LogEntry:
    """Immutable log entry representation with CRC32C checksum verification.
    
    Binary Layout (struct format: '!Q I I I 32s'):
    - term: uint64 (8 bytes)
    - index: uint32 (4 bytes)
    - opcode: uint32 (4 bytes)
    - payload_length: uint32 (4 bytes)
    - payload: raw bytes (variable length, padded/exact)
    - crc32c: uint32 stored separately or validated via payload hash
    """
    term: int
    index: int
    opcode: int
    payload: bytes
    crc32: int = field(default=0)

    def __post_init__(self) -> None:
        if self.crc32 == 0:
            # Compute CRC32C over term, index, opcode, and payload
            calculated = self._compute_checksum(
                self.term, self.index, self.opcode, self.payload
            )
            object.__setattr__(self, 'crc32', calculated)
        else:
            current = self._compute_checksum(
                self.term, self.index, self.opcode, self.payload
            )
            if current != self.crc32:
                raise ChecksumError(
                    f"CRC32C mismatch for LogEntry index={self.index}, term={self.term}: "
                    f"expected {self.crc32:08X}, got {current:08X}"
                )

    @staticmethod
    def _compute_checksum(term: int, index: int, opcode: int, payload: bytes) -> int:
        header = struct.pack('!Q I I', term, index, opcode)
        hasher = zlib.crc32(header)
        hasher = zlib.crc32(payload, hasher)
        return hasher & 0xFFFFFFFF

    def serialize(self) -> bytes:
        payload_len = len(self.payload)
        header = struct.pack('!Q I I I I', self.term, self.index, self.opcode, payload_len, self.crc32)
        return header + self.payload

    @classmethod
    def deserialize(cls, buffer: bytes) -> 'LogEntry':
        if len(buffer) < 24:
            raise ValueError("Buffer too small to contain LogEntry header.")
        term, index, opcode, payload_len, stored_crc = struct.unpack('!Q I I I I', buffer[:24])
        if len(buffer) < 24 + payload_len:
            raise ValueError("Buffer truncated relative to payload length specification.")
        payload = buffer[24:24 + payload_len]
        
        # Verify checksum during deserialization
        calculated = cls._compute_checksum(term, index, opcode, payload)
        if calculated != stored_crc:
            raise ChecksumError(
                f"Deserialization CRC32C failure at index {index}: "
                f"stored {stored_crc:08X}, calculated {calculated:08X}"
            )
        
        return cls(term=term, index=index, opcode=opcode, payload=payload, crc32=stored_crc)


class PersistentState:
    """Atomically synced struct tracking currentTerm, votedFor, and log compaction indices."""

    def __init__(self, filepath: str) -> None:
        self.filepath = filepath
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        self.last_included_index: int = 0
        self.last_included_term: int = 0
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        async with self._lock:
            try:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self._sync_read)
            except FileNotFoundError:
                await loop.run_in_executor(None, self._sync_write)

    def _sync_read(self) -> None:
        with open(self.filepath, 'rb') as f:
            data = f.read()
        if not data:
            return
        # Layout: term(Q), voted_for_len(I), voted_for(bytes), last_inc_idx(Q), last_inc_term(Q)
        term, vf_len, offset = struct.unpack('!Q I', data[:12])
        voted_for_bytes = data[12:12 + vf_len]
        voted_for = voted_for_bytes.decode('utf-8') if vf_len > 0 else None
        last_inc_idx, last_inc_term = struct.unpack('!Q Q', data[12 + vf_len:12 + vf_len + 16])
        
        self.current_term = term
        self.voted_for = voted_for
        self.last_included_index = last_inc_idx
        self.last_included_term = last_inc_term

    def _sync_write(self) -> None:
        vf_bytes = (self.voted_for or "").encode('utf-8')
        vf_len = len(vf_bytes)
        header = struct.pack('!Q I', self.current_term, vf_len)
        trailer = struct.pack('!Q Q', self.last_included_index, self.last_included_term)
        data = header + vf_bytes + trailer
        
        # Atomic write via temporary file replacement
        temp_path = f"{self.filepath}.tmp"
        with open(temp_path, 'wb') as f:
            f.write(data)
            f.flush()
        import os
        os.replace(temp_path, self.filepath)

    async def update(self, current_term: int, voted_for: Optional[str]) -> None:
        async with self._lock:
            self.current_term = current_term
            self.voted_for = voted_for
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._sync_write)

    async def update_snapshot_meta(self, last_included_index: int, last_included_term: int) -> None:
        async with self._lock:
            self.last_included_index = last_included_index
            self.last_included_term = last_included_term
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._sync_write)


class VolatileState:
    """Thread-safe dictionaries tracking commitIndex, lastApplied, nextIndex[], and matchIndex[] across peers."""

    def __init__(self, peer_ids: List[str]) -> None:
        self.commit_index: int = 0
        self.last_applied: int = 0
        self._next_index: Dict[str, int] = {peer: 1 for peer in peer_ids}
        self._match_index: Dict[str, int] = {peer: 0 for peer in peer_ids}
        self._lock = asyncio.Lock()

    async def get_commit_index(self) -> int:
        async with self._lock:
            return self.commit_index

    async def set_commit_index(self, index: int) -> None:
        async with self._lock:
            if index > self.commit_index:
                self.commit_index = index

    async def get_last_applied(self) -> int:
        async with self._lock:
            return self.last_applied

    async def set_last_applied(self, index: int) -> None:
        async with self._lock:
            self.last_applied = index

    async def get_next_index(self, peer: str) -> int:
        async with self._lock:
            return self._next_index.get(peer, 1)

    async def set_next_index(self, peer: str, index: int) -> None:
        async with self._lock:
            self._next_index[peer] = max(1, index)

    async def get_match_index(self, peer: str) -> int:
        async with self._lock:
            return self._match_index.get(peer, 0)

    async def set_match_index(self, peer: str, index: int) -> None:
        async with self._lock:
            self._match_index[peer] = max(0, index)

    async def reset_peer_indices(self, peer_ids: List[str], next_idx: int) -> None:
        async with self._lock:
            for peer in peer_ids:
                self._next_index[peer] = next_idx
                self._match_index[peer] = 0


class StateMachine(ABC):
    """Abstract base state machine interface enforcing transactional state application with deterministic snapshot serialization."""

    @abstractmethod
    async def apply(self, opcode: int, payload: bytes) -> bytes:
        """Apply a single committed log entry transactionally, returning execution results."""
        pass

    @abstractmethod
    async def create_snapshot(self) -> bytes:
        """Serialize the complete state machine snapshot deterministically into raw bytes."""
        pass

    @abstractmethod
    async def restore_snapshot(self, snapshot_data: bytes) -> None:
        """Restore state machine state from serialized snapshot data safely."""
        pass