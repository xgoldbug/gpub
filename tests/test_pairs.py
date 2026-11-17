#!/usr/bin/env python3
"""test_pairs.py — group ("pair") resources gate for GPUB. Exits NON-ZERO on any failure.
Fixture db (gpub_test, DROPPED and recreated), coordinator on 127.0.0.1:8767, no GPU, no production database touched.
tests/run_all.py runs test_phase1.py and test_agent.py first.
  P1 pair booking -> pair row + one exclusive member per member; job on the DRIVER member only; grace on the driver, 0 elsewhere
  P2 a live shared hold on ONE member refuses the pair with 409 and leaves NO partial rows
  P3 cancel of a booked pair cancels its members
  P4 an active pair released by the owner releases its members (end_reason *_pair)
  P5 extend refused for the group when one member's next slot is taken; granted (all rows move) once it is freed
  P6 an RRULE template on a pair expands occurrences WITH members
  P7 earliest-fit on a pair waits for the busiest member
  P8 shared booking on a pair -> 422
"""
from __future__ import annotations
import datetime as dt, json, os, subprocess, sys, tempfile, time
from pathlib import Path
import httpx, psycopg
from psycopg.rows import dict_row

HERE = Path(__file__).resolve().parent
PKG = HERE.parent / "gpub"
sys.path.insert(0, str(PKG))
import coordinator as co  # noqa: E402

PY = sys.executable; DB = "gpub_test"; PORT = 8767; BIND = "127.0.0.1"; URL = f"http://{BIND}:{PORT}"; TICK = 2.0
results: list[tuple[str, bool, str]] = []

def check(name, ok, detail=""):
    results.append((name, bool(ok), detail)); print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""), flush=True)

def api(tok, method, path, **kw):
    h = {"Authorization": f"Bearer {tok}"} if tok else {}
    return httpx.request(method, URL + path, headers=h, timeout=20, **kw)

def rows(sql, *args):
    with psycopg.connect(f"dbname={DB}", autocommit=True, row_factory=dict_row) as c:
        return c.execute(sql, args).fetchall()

def wait(pred, timeout, step=1.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        v = pred()
        if v: return v
        time.sleep(step)
    return None

def part2() -> None:
    with psycopg.connect("dbname=postgres", autocommit=True) as c:
        c.execute(f"DROP DATABASE IF EXISTS {DB} WITH (FORCE)")
    lock = Path(tempfile.gettempdir()) / "gpub_test_gpu.lock"; lock.unlink(missing_ok=True)
    co.ensure_db(DB); co.apply_schema(DB); co.seed(DB, None, str(lock))
    with psycopg.connect(f"dbname={DB}", autocommit=True) as c:
        c.execute("INSERT INTO machines (machine) VALUES ('spark-b') ON CONFLICT DO NOTHING")
        c.execute("""INSERT INTO resources (resource_id, host_machine, model, capacity_gb) VALUES
                     ('spark-b-gx10','spark-b','GB10',110) ON CONFLICT DO NOTHING""")
        c.execute("""INSERT INTO resources (resource_id, host_machine, model, capacity_gb, members) VALUES
                     ('gb10-duo','spark-a','PAIR',220, ARRAY['spark-a-gb10','spark-b-gx10']) ON CONFLICT DO NOTHING""")
    tok = co.register(DB, "headnode", BIND, "linux-wsl2")
    scratch = Path(tempfile.mkdtemp(prefix="gpub_pairs_test_"))
    srv = subprocess.Popen([PY, str(PKG / "coordinator.py"), "--db", DB, "serve", "--bind", BIND, "--port", str(PORT),
                            "--tick", str(TICK)], stdout=open(scratch / "co.log", "w"), stderr=subprocess.STDOUT)
    try:
        assert wait(_up, 30), "coordinator did not come up"
        now = dt.datetime.now(dt.UTC).replace(microsecond=0)
        T = lambda m: (now + dt.timedelta(minutes=m)).isoformat()
        # P1
        r = api(tok, "POST", "/bookings", json={"resource_id": "gb10-duo", "mode": "exclusive", "start": T(60), "duration_min": 30,
                                                "purpose": "P1 duo", "grace_min": 7, "job": {"cmd": "echo duo"}})
        check("P1 pair booking accepted", r.status_code == 201, r.text[:120])
        pid = r.json()["id"]
        mem = rows("SELECT * FROM bookings WHERE pair_id=%s ORDER BY resource_id", pid)
        check("P1 two exclusive members created", len(mem) == 2 and all(m["mode"] == "exclusive" for m in mem), str([(m["resource_id"], m["mode"]) for m in mem]))
        drv = [m for m in mem if m["resource_id"] == "spark-a-gb10"]; oth = [m for m in mem if m["resource_id"] == "spark-b-gx10"]
        check("P1 job on the driver member only", drv and drv[0]["job"] == {"cmd": "echo duo"} and oth and oth[0]["job"] is None)
        pr = rows("SELECT grace_min FROM bookings WHERE id=%s", pid)[0]
        check("P1 grace: driver 7, other 0, pair 0", drv[0]["grace_min"] == 7 and oth[0]["grace_min"] == 0 and pr["grace_min"] == 0,
              f"driver {drv[0]['grace_min']} other {oth[0]['grace_min']} pair {pr['grace_min']}")
        # P2
        n0 = rows("SELECT count(*) AS n FROM bookings")[0]["n"]
        r = api(tok, "POST", "/bookings", json={"resource_id": "spark-b-gx10", "mode": "shared", "vram_budget_gb": 20, "start": T(120),
                                                "duration_min": 60, "purpose": "P2 blocker"})
        check("P2 shared hold on one member placed", r.status_code == 201, r.text[:120])
        r = api(tok, "POST", "/bookings", json={"resource_id": "gb10-duo", "mode": "exclusive", "start": T(130), "duration_min": 30, "purpose": "P2 duo"})
        check("P2 pair over a busy member refused 409", r.status_code == 409, r.text[:120])
        n1 = rows("SELECT count(*) AS n FROM bookings")[0]["n"]
        check("P2 no partial rows left behind", n1 == n0 + 1, f"{n0}->{n1}")
        # P3
        r = api(tok, "POST", f"/bookings/{pid}/cancel")
        st = rows("SELECT state FROM bookings WHERE pair_id=%s", pid)
        check("P3 cancel pair cancels members", r.status_code == 200 and all(s["state"] == "cancelled" for s in st), str(st))
        # P4
        r = api(tok, "POST", "/bookings", json={"resource_id": "gb10-duo", "mode": "exclusive", "start": T(0), "duration_min": 10, "purpose": "P4 duo now", "grace_min": 0})
        check("P4 pair booked now", r.status_code == 201, r.text[:120]); p4 = r.json()["id"]
        ok = wait(lambda: all(s["state"] == "active" for s in rows("SELECT state FROM bookings WHERE id=%s OR pair_id=%s", p4, p4)), 20)
        check("P4 pair + members all active", bool(ok))
        r = api(tok, "POST", f"/bookings/{p4}/release")
        st = rows("SELECT state, end_reason FROM bookings WHERE pair_id=%s", p4)
        check("P4 release pair releases members", r.status_code == 200 and all(s["state"] == "released" and s["end_reason"] == "holder_release_pair" for s in st), str(st))
        # P5
        r = api(tok, "POST", "/bookings", json={"resource_id": "gb10-duo", "mode": "exclusive", "start": T(0), "duration_min": 10, "purpose": "P5 duo", "grace_min": 0})
        p5 = r.json()["id"]
        wait(lambda: rows("SELECT state FROM bookings WHERE id=%s", p5)[0]["state"] == "active", 20)
        blk = api(tok, "POST", "/bookings", json={"resource_id": "spark-b-gx10", "mode": "exclusive", "start": T(10), "duration_min": 30, "purpose": "P5 blocker"})
        check("P5 blocker on a member right after the pair", blk.status_code == 201, blk.text[:120])
        r = api(tok, "POST", f"/bookings/{p5}/extend", json={"minutes": 15})
        check("P5 extend refused while a member's next slot is taken", r.status_code == 409, r.text[:120])
        api(tok, "POST", f"/bookings/{blk.json()['id']}/cancel")
        r = api(tok, "POST", f"/bookings/{p5}/extend", json={"minutes": 15})
        ends = rows("SELECT upper(slot) AS e FROM bookings WHERE id=%s OR pair_id=%s", p5, p5)
        check("P5 extend granted moves the whole group", r.status_code == 200 and len({e["e"] for e in ends}) == 1 and len(ends) == 3, str([e["e"].isoformat() for e in ends]))
        api(tok, "POST", f"/bookings/{p5}/release")
        # P6
        r = api(tok, "POST", "/bookings", json={"resource_id": "gb10-duo", "mode": "exclusive", "start": T(240), "duration_min": 20,
                                                "purpose": "P6 nightly duo", "rrule": "FREQ=DAILY;COUNT=3", "tz": "UTC"})
        check("P6 pair template accepted", r.status_code == 201, r.text[:120]); tpl = r.json()["id"]
        occ = wait(lambda: rows("SELECT id FROM bookings WHERE parent_id=%s AND state='booked'", tpl) or None, 15)
        memo = rows("SELECT count(*) AS n FROM bookings WHERE pair_id = ANY(%s)", [o["id"] for o in (occ or [])])[0]["n"] if occ else 0
        check("P6 occurrences carry members", occ and memo == 2 * len(occ), f"occ {len(occ or [])} members {memo}")
        # P7
        r = api(tok, "POST", "/bookings", json={"resource_id": "spark-a-gb10", "mode": "exclusive", "start": T(0), "duration_min": 3, "purpose": "P7 busy member", "grace_min": 0})
        r = api(tok, "POST", "/bookings/earliest", json={"resource_id": "gb10-duo", "mode": "exclusive", "duration_min": 5, "purpose": "P7 duo earliest", "max_wait_min": 30})
        s = dt.datetime.fromisoformat(r.json()["start"]) if r.status_code == 201 else None
        check("P7 earliest pair starts after the busy member", r.status_code == 201 and s is not None and s >= now + dt.timedelta(minutes=3) - dt.timedelta(seconds=2), r.text[:120])
        # P8
        r = api(tok, "POST", "/bookings", json={"resource_id": "gb10-duo", "mode": "shared", "vram_budget_gb": 10, "start": T(600), "duration_min": 5, "purpose": "P8"})
        check("P8 shared booking on a pair -> 422", r.status_code == 422, r.text[:120])
    finally:
        srv.terminate()
        try: srv.wait(10)
        except Exception: srv.kill()

def _up():
    try: return httpx.get(URL + "/time", timeout=2).status_code == 200
    except Exception: return False

def main() -> int:
    print("=== test_pairs: group resources ===", flush=True)
    part2()
    failed = [r for r in results if not r[1]]
    print(f"\npairs: {len(results) - len(failed)}/{len(results)} checks passed")
    if failed:
        print("GATE: FAIL — " + "; ".join(n for n, _, _ in failed)); return 1
    print("GATE: PASS"); return 0

if __name__ == "__main__":
    sys.exit(main())
