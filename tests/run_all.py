#!/usr/bin/env python3
"""run_all.py — run the GPUB acceptance gates in order (phase1, agent, pairs, mesh). Exits non-zero if any gate fails.

Every gate DROPS and recreates the fixture database `gpub_test` and starts its own coordinator on 127.0.0.1:8767,
so run them on a machine with a local Postgres and nothing else on that port. Takes roughly 10 minutes.
"""
import subprocess, sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
GATES = ["test_phase1.py", "test_agent.py", "test_pairs.py", "test_mesh.py"]


def main() -> int:
    summary = []
    for g in GATES:
        print(f"\n######## {g} ########", flush=True)
        t0 = time.time()
        rc = subprocess.run([sys.executable, str(HERE / g)], cwd=str(HERE)).returncode
        summary.append((g, rc, time.time() - t0))
    print("\n######## summary ########")
    for g, rc, secs in summary:
        print(f"{g:16} {'PASS' if rc == 0 else f'FAIL (exit {rc})':16} {secs:6.0f} s")
    failed = [g for g, rc, _ in summary if rc != 0]
    print("GATE: " + ("PASS (phase1 + agent + pairs + mesh)" if not failed else "FAIL — " + ", ".join(failed)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
