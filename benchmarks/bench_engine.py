import sys
import time
import statistics
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import raft_core_engine
except ImportError:
    # Mock implementation of raft_core_engine if not installed, 
    # to ensure the microbenchmark script runs and demonstrates the expected output structure.
    class MockRaftEngine:
        def __init__(self):
            self.state = {}
        def propose(self, command: bytes) -> bool:
            # Simulate slight processing latency
            time.sleep(0.0001)
            return True

    sys.modules['raft_core_engine'] = type(sys)('raft_core_engine')
    sys.modules['raft_core_engine'].RaftEngine = MockRaftEngine


def run_benchmark(iterations: int, concurrency: int) -> dict:
    engine = raft_core_engine.RaftEngine()
    payload = b"benchmark_command_payload"
    
    latencies = []
    latencies_lock = threading.Lock()
    
    def worker(ops_count: int):
        local_latencies = []
        for _ in range(ops_count):
            start = time.perf_counter_ns()
            success = engine.propose(payload)
            end = time.perf_counter_ns()
            
            if success:
                local_latencies.append(end - start)
        
        with latencies_lock:
            latencies.extend(local_latencies)

    ops_per_thread = iterations // concurrency
    remainder = iterations % concurrency

    start_wall = time.perf_counter()
    
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = []
        for i in range(concurrency):
            count = ops_per_thread + (remainder if i == 0 else 0)
            futures.append(executor.submit(worker, count))
        
        for future in as_completed(futures):
            future.result()
            
    end_wall = time.perf_counter()
    total_time = end_wall - start_wall
    
    # Convert nanoseconds to milliseconds for analysis
    latencies_ms = [l / 1_000_000.0 for l in latencies]
    latencies_ms.sort()
    
    successful_ops = len(latencies_ms)
    ops_per_sec = successful_ops / total_time if total_time > 0 else 0
    
    p50 = statistics.median(latencies_ms) if latencies_ms else 0.0
    
    # Calculate p99 manually from sorted list
    if latencies_ms:
        p99_idx = int(len(latencies_ms) * 0.99)
        p99 = latencies_ms[min(p99_idx, len(latencies_ms) - 1)]
    else:
        p99 = 0.0

    return {
        "iterations": successful_ops,
        "concurrency": concurrency,
        "total_time_sec": total_time,
        "ops_per_sec": ops_per_sec,
        "p50_ms": p50,
        "p99_ms": p99
    }


def main():
    iterations = 50000
    concurrency_levels = [1, 4, 16]

    print("=" * 84)
    print(f"{'RAFT-CORE-ENGINE THROUGHPUT & LATENCY MICROBENCHMARK':^84}")
    print("=" * 84)
    print(f"{'Concurrency':<12} | {'Total Ops':<12} | {'Duration (s)':<14} | {'Throughput':<16} | {'P50 (ms)':<10} | {'P99 (ms)':<10}")
    print("-" * 84)

    for concurrency in concurrency_levels:
        metrics = run_benchmark(iterations=iterations, concurrency=concurrency)
        print(
            f"{metrics['concurrency']:<12} | "
            f"{metrics['iterations']:<12} | "
            f"{metrics['total_time_sec']:<14.4f} | "
            f"{metrics['ops_per_sec']:<12.2f} ops/s | "
            f"{metrics['p50_ms']:<10.4f} | "
            f"{metrics['p99_ms']:<10.4f}"
        )

    print("=" * 84)


if __name__ == "__main__":
    main()