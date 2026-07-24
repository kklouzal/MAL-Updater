from __future__ import annotations

import json
import multiprocessing
import tempfile
import time
import unittest
from pathlib import Path

from mal_updater.provider_niceness import ProviderRequestGate, retry_after_seconds, retry_delay_seconds


def _process_gate_worker(state_dir: str, output: str, ready, start) -> None:
    gate = ProviderRequestGate("cross-process-test", Path(state_dir), 0.06, 0.0)
    ready.put(True)
    start.wait()
    gate.wait_turn()
    with open(output, "a", encoding="utf-8") as handle:
        handle.write(f"{time.time()}\n")


class ProviderRequestGateTests(unittest.TestCase):
    def test_cross_instance_gate_uses_shared_timestamp_with_injected_clock(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            now = [100.0]
            sleeps: list[float] = []

            def clock() -> float:
                return now[0]

            def sleep(seconds: float) -> None:
                sleeps.append(seconds)
                now[0] += seconds

            first = ProviderRequestGate("mal", Path(td), 1.0, 0.0, clock=clock, sleep=sleep)
            second = ProviderRequestGate("mal", Path(td), 1.0, 0.0, clock=clock, sleep=sleep)
            first.wait_turn()
            now[0] = 100.25
            second.wait_turn()

            self.assertEqual([0.75], sleeps)
            payload = json.loads(second.state_path.read_text(encoding="utf-8"))
            self.assertEqual(101.0, payload["last_request_started_at"])

    def test_gate_serializes_two_processes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            output = str(Path(td) / "starts.txt")
            context = multiprocessing.get_context("spawn")
            ready = context.Queue()
            start = context.Event()
            processes = [context.Process(target=_process_gate_worker, args=(td, output, ready, start)) for _ in range(2)]
            for process in processes:
                process.start()
            ready.get(timeout=5)
            ready.get(timeout=5)
            start.set()
            for process in processes:
                process.join(timeout=5)
                self.assertEqual(0, process.exitcode)
            starts = sorted(float(value) for value in Path(output).read_text(encoding="utf-8").splitlines())
            self.assertGreaterEqual(starts[1] - starts[0], 0.045)

    def test_retry_after_and_exponential_jitter_are_capped(self) -> None:
        self.assertEqual(12.0, retry_after_seconds("12"))
        self.assertEqual(10.0, retry_delay_seconds(1, retry_after="120", cap_seconds=10.0))
        self.assertEqual(4.5, retry_delay_seconds(3, base_seconds=1.0, jitter_seconds=1.0, cap_seconds=10.0, uniform=lambda _a, _b: 0.5))


if __name__ == "__main__":
    unittest.main()
