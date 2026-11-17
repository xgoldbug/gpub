#!/usr/bin/env python3
"""test_mesh.py — gate for mesh description tooling and the dashboard route. Exits non-zero on any failure.

M1 presets embedded in viz/gpubdash.html are identical to examples/presets.json
M2 examples/mesh.example.json validates; a broken mesh is rejected with the expected messages
M3 the generated SQL loads into a fresh fixture db (gpub_test), twice (idempotent), with groups and members intact
M4 the coordinator serves gpubdash at /dash (127.0.0.1:8767)
"""
from __future__ import annotations
import json, re, subprocess, sys, time
from pathlib import Path

import httpx, psycopg

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT / "gpub")); sys.path.insert(0, str(ROOT / "tools"))
import coordinator as co   # noqa: E402
import mesh_to_sql as m2s  # noqa: E402

PY = sys.executable; DB = "gpub_test"; PORT = 8767; URL = f"http://127.0.0.1:{PORT}"
results: list[tuple[str, bool, str]] = []


def check(name, ok, detail=""):
    results.append((name, bool(ok), detail)); print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""), flush=True)


def main() -> int:
    # M1
    html = (ROOT / "viz" / "gpubdash.html").read_text()
    m = re.search(r"/\*PRESETS-BEGIN\*/const PRESETS = (.*?);/\*PRESETS-END\*/", html, re.S)
    file_presets = json.loads((ROOT / "examples" / "presets.json").read_text())
    check("M1 dashboard presets == examples/presets.json", m and json.loads(m.group(1)) == file_presets)
    # M2
    presets, links = m2s.load_presets()
    machines, res, groups, errs = m2s.resolve(json.loads((ROOT / "examples" / "mesh.example.json").read_text()), presets, links)
    check("M2 example mesh validates", not errs and len(res) == 7 and len(groups) == 3, "; ".join(errs))
    bad = {"machines": [{"name": "A b"}], "resources": [{"id": "x", "machine": "nope", "preset": "rtx-3090", "budget_gb": 99}],
           "groups": [{"id": "g", "members": ["x"], "link": "warp"}]}
    _, _, _, e2 = m2s.resolve(bad, presets, links)
    want = ["machine name 'A b'", "machine 'nope' is not in machines", "budget_gb 99 must be", "needs 2+ distinct members", "unknown link 'warp'"]
    check("M2 broken mesh rejected with clear messages", all(any(w in e for e in e2) for w in want), "; ".join(e2))
    # M3
    with psycopg.connect("dbname=postgres", autocommit=True) as c:
        c.execute(f"DROP DATABASE IF EXISTS {DB} WITH (FORCE)")
    co.ensure_db(DB); co.apply_schema(DB)
    sql = m2s.to_sql(res, groups)
    for _ in range(2):
        with psycopg.connect(f"dbname={DB}", autocommit=True) as c:
            c.execute(sql)
    with psycopg.connect(f"dbname={DB}") as c:
        n = c.execute("SELECT count(*) FROM resources").fetchone()[0]
        duo = c.execute("SELECT members, capacity_gb FROM resources WHERE resource_id='gb10-duo'").fetchone()
        mac = c.execute("SELECT host_machine, capacity_gb FROM resources WHERE resource_id='mac-pair'").fetchone()
    check("M3 SQL loads twice, 10 rows (7 + 3 groups)", n == 10, f"rows={n}")
    check("M3 group members and budgets kept", duo and duo[0] == ["spark-a-gb10", "spark-b-gb10"] and float(duo[1]) == 220
          and mac and mac[0] == "studio" and float(mac[1]) == 188, f"duo={duo} mac={mac}")
    # M4
    srv = subprocess.Popen([PY, str(ROOT / "gpub" / "coordinator.py"), "--db", DB, "serve", "--bind", "127.0.0.1", "--port", str(PORT),
                            "--tick", "5"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        r = None
        for _ in range(40):
            try:
                r = httpx.get(URL + "/dash", timeout=3); break
            except Exception:
                time.sleep(0.5)
        check("M4 /dash serves gpubdash", r is not None and r.status_code == 200 and "gpubdash" in r.text and "PRESETS-BEGIN" in r.text,
              f"status={getattr(r, 'status_code', None)}")
        st = httpx.get(URL + "/status", timeout=5).json()
        check("M4 /status lists the mesh resources", len(st["resources"]) == 10)
    finally:
        srv.terminate()
        try: srv.wait(5)
        except subprocess.TimeoutExpired: srv.kill()
    failed = [r for r in results if not r[1]]
    print(f"\nmesh gate: {len(results) - len(failed)}/{len(results)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
