#!/usr/bin/env python3
"""test_phase1.py — core acceptance gate for GPUB. Exits NON-ZERO on any failure.
Runs against an isolated fixture database (gpub_test, DROPPED and recreated) and a coordinator on 127.0.0.1:8767
with a SCRATCH lockfile in the temp dir — it never touches a production database or a real lockfile.
Needs a local Postgres the current user can create databases in (libpq defaults / PG* env vars).

Covers:  T1 exclusive overlap rejected BY POSTGRES via raw SQL · T2 shared-sum + exclusive/shared rejected ·
T3 kill -9 of `gpub run` -> lease expires, slot released, LEASE_EXPIRED to holder + broadcast ·
T7a RRULE expansion in America/Chicago (wall-clock 02:00 kept in UTC terms across DST) · T10 identity spoof rejected.
Plus invariants: earliest-fit queue, optional external lockfile wrap (acquire/delay/release), DELETE blocked, audit.
"""
from __future__ import annotations
import datetime as dt, json, os, signal, subprocess, sys, tempfile, time
from pathlib import Path

import httpx, psycopg
from psycopg import errors as pgerr

HERE = Path(__file__).resolve().parent
PKG = HERE.parent / "gpub"
sys.path.insert(0, str(PKG))
import coordinator as co  # noqa: E402

PY = sys.executable
DB = "gpub_test"            # fixture database: dropped and recreated by fresh_db()
PORT = 8767
BIND = "127.0.0.1"
OTHER_IP = "127.0.0.2"      # registered IP of the second machine; requests never come from it (identity test T10)
URL = f"http://{BIND}:{PORT}"
TICK = 2.0
LOCK = Path(tempfile.gettempdir()) / "gpub_test_gpu.lock"

results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""), flush=True)


def fresh_db() -> None:
    with psycopg.connect("dbname=postgres", autocommit=True) as c:
        c.execute(f"DROP DATABASE IF EXISTS {DB} WITH (FORCE)")   # scratch fixture only; never the ledger
    co.ensure_db(DB); co.apply_schema(DB); co.seed(DB, None, str(LOCK))
    LOCK.unlink(missing_ok=True)


def api(tok: str | None, method: str, path: str, **kw) -> httpx.Response:
    h = {"Authorization": f"Bearer {tok}"} if tok else {}
    return httpx.request(method, URL + path, headers=h, timeout=20, **kw)


def wait_state(bid: int, want: set[str], timeout: float) -> str:
    t0 = time.time()
    while time.time() - t0 < timeout:
        st = api(None, "GET", f"/bookings/{bid}").json()["booking"]["state"]
        if st in want:
            return st
        time.sleep(1)
    return st


def main() -> int:
    fresh_db()
    tok_head = co.register(DB, "headnode", BIND, "linux-wsl2")
    tok_rtx = co.register(DB, "rtxbox", OTHER_IP, "linux-wsl2")
    env = dict(os.environ, GPUB_DB=DB)
    srv = subprocess.Popen([PY, str(PKG / "coordinator.py"), "--db", DB, "serve", "--bind", BIND,
                            "--port", str(PORT), "--tick", str(TICK)], env=env,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(40):
            try:
                httpx.get(URL + "/time", timeout=2); break
            except httpx.HTTPError:
                time.sleep(0.25)
        else:
            check("coordinator starts", False, "no /time after 10 s"); return 1
        check("coordinator starts", True, f"{URL} db={DB}")

        far = dt.datetime(2031, 3, 3, 12, 0, tzinfo=dt.UTC)   # far-future window: no scheduler interaction
        # ---------------- T1: raw SQL, bypassing the API -----------------------------------------
        with psycopg.connect(f"dbname={DB}") as c:
            c.execute("INSERT INTO bookings (resource_id, owner_machine, mode, slot) VALUES "
                      "('rtxbox-5090','headnode','exclusive', tstzrange(%s, %s, '[)'))", (far, far + dt.timedelta(hours=2)))
            c.commit()
            try:
                c.execute("INSERT INTO bookings (resource_id, owner_machine, mode, slot) VALUES "
                          "('rtxbox-5090','rtxbox','exclusive', tstzrange(%s, %s, '[)'))",
                          (far + dt.timedelta(hours=1), far + dt.timedelta(hours=3)))
                c.commit(); check("T1 exclusive overlap rejected by Postgres (raw SQL)", False, "second insert succeeded")
            except pgerr.ExclusionViolation as e:
                c.rollback(); check("T1 exclusive overlap rejected by Postgres (raw SQL)", True, "no_exclusive_overlap" if "no_exclusive_overlap" in str(e) else str(e)[:80])
            # adjacent [) ranges must NOT conflict
            c.execute("INSERT INTO bookings (resource_id, owner_machine, mode, slot) VALUES "
                      "('rtxbox-5090','rtxbox','exclusive', tstzrange(%s, %s, '[)'))",
                      (far + dt.timedelta(hours=2), far + dt.timedelta(hours=3)))
            c.commit(); check("T1b back-to-back exclusive slots allowed", True)
            # audit rows exist and DELETE is blocked
            n = c.execute("SELECT count(*) FROM audit_log WHERE action='booking_created'").fetchone()[0]
            check("audit_log rows written by trigger", n == 2, f"{n} rows")
            try:
                c.execute("DELETE FROM bookings"); c.commit(); check("DELETE blocked on ledger", False)
            except pgerr.RaiseException:
                c.rollback(); check("DELETE blocked on ledger", True)

        # ---------------- T2: shared sums and exclusive/shared --------------------------------------
        base = far + dt.timedelta(days=1)
        def book(tok, **p):
            body = {"resource_id": "rtxbox-5090", "mode": "shared", "priority": "interactive", "duration_min": 120,
                    "purpose": "T2"} | p
            return api(tok, "POST", "/bookings", json=body)
        r1 = book(tok_head, vram_budget_gb=20, start=base.isoformat())
        check("T2 shared 20G booked", r1.status_code == 201, r1.text[:100])
        r2 = book(tok_head, vram_budget_gb=13, start=(base + dt.timedelta(minutes=30)).isoformat())
        check("T2 shared 20+13 > 32 rejected", r2.status_code == 409, r2.text[:120])
        r3 = book(tok_head, vram_budget_gb=12, start=(base + dt.timedelta(minutes=30)).isoformat())
        check("T2 shared 20+12 = 32 accepted (exact peak, not conservative)", r3.status_code == 201, r3.text[:100])
        r4 = book(tok_head, mode="exclusive", vram_budget_gb=None, start=(base + dt.timedelta(minutes=60)).isoformat())
        check("T2 exclusive over shared rejected", r4.status_code == 409, r4.text[:120])
        r5 = book(tok_head, vram_budget_gb=1, start=(far + dt.timedelta(minutes=30)).isoformat())
        check("T2 shared over exclusive rejected", r5.status_code == 409, r5.text[:120])
        # a shared booking that overlaps r1 only AFTER r3 ends must see peak 20 (not 32): exactness check
        r6 = book(tok_head, vram_budget_gb=12, start=(base + dt.timedelta(minutes=150)).isoformat(), duration_min=30)
        check("T2 shared peak computed per instant (12G fits after 12G ends)", r6.status_code == 201, r6.text[:100])

        # ---------------- T10: identity --------------------------------------------------------------
        r = book(tok_rtx, vram_budget_gb=1, start=(far + dt.timedelta(days=2)).isoformat())
        check("T10 rtxbox's token presented from headnode's IP rejected", r.status_code == 403, r.text[:120])
        r = api("gpub_bogus", "POST", "/bookings", json={"resource_id": "rtxbox-5090", "duration_min": 10})
        check("T10 unknown token rejected", r.status_code == 401, r.text[:80])

        # ---------------- earliest-fit queue --------------------------------------------------------
        r_now = api(tok_head, "POST", "/bookings", json={"resource_id": "spark-a-gb10", "mode": "exclusive",
                                                            "duration_min": 2, "purpose": "queue head", "grace_min": 0})
        check("queue: exclusive now on spark-a", r_now.status_code == 201, r_now.text[:100])
        r_nw = api(tok_head, "POST", "/bookings/earliest", json={"resource_id": "spark-a-gb10", "mode": "exclusive",
                                                                   "duration_min": 1, "max_wait_min": 0})
        check("queue: earliest with no wait -> 409", r_nw.status_code == 409, r_nw.text[:100])
        r_w = api(tok_head, "POST", "/bookings/earliest", json={"resource_id": "spark-a-gb10", "mode": "exclusive",
                                                                  "duration_min": 1, "max_wait_min": 10, "grace_min": 0})
        ok = r_w.status_code == 201 and r_w.json()["start"] == r_now.json()["end"]
        check("queue: earliest-fit starts exactly when the head ends", ok, f"{r_w.status_code} {r_w.text[:100]}")

        # ---------------- optional external lockfile wrap -------------------------------------------
        LOCK.write_text(json.dumps({"holder": "legacy-grade", "host": "elsewhere", "pid": 1,
                                    "started": dt.datetime.now().isoformat(timespec="seconds"), "note": "fixture"}))
        r_ox = api(tok_head, "POST", "/bookings", json={"resource_id": "rtxbox-5090", "mode": "exclusive",
                                                           "duration_min": 3, "purpose": "wrap test", "grace_min": 0})
        check("lockfile wrap: booking on wrapped resource accepted", r_ox.status_code == 201, r_ox.text[:100])
        bid = r_ox.json().get("id", -1)
        time.sleep(TICK * 2 + 1)
        st = api(None, "GET", f"/bookings/{bid}").json()["booking"]["state"]
        sig = [s for s in api(None, "GET", "/signals", params={"limit": 50}).json() if s["type"] == "SLOT_DELAYED" and s["booking_id"] == bid]
        check("lockfile wrap: foreign lock delays the slot (stays booked, SLOT_DELAYED)", st == "booked" and sig, f"state={st} signals={len(sig)}")
        LOCK.unlink()
        st = wait_state(bid, {"active"}, TICK * 3 + 2)
        info = json.loads(LOCK.read_text()) if LOCK.exists() else None
        check("lockfile wrap: lock freed -> slot activates and lock is held as gpub:<id> by coordinator pid",
              st == "active" and info and info["holder"] == f"gpub:{bid}" and info["pid"] == srv.pid, f"state={st} lock={info}")
        r = api(tok_head, "POST", f"/bookings/{bid}/release")
        time.sleep(0.5)
        check("lockfile wrap: holder release frees the lock", r.status_code == 200 and not LOCK.exists(), r.text[:80])
        r = api(tok_rtx, "POST", f"/bookings/{bid}/release")
        check("release by non-owner rejected", r.status_code in (403,), r.text[:80])

        # ---------------- T3: kill -9 the gpub run client ------------------------------------------
        cfgp = Path(tempfile.mkdtemp()) / "config.json"
        cfgp.write_text(json.dumps({"url": URL, "machine": "headnode", "token": tok_head})); cfgp.chmod(0o600)
        t3log = open(cfgp.parent / "t3.log", "w")
        t3 = subprocess.Popen([PY, str(PKG / "cli.py"), "run", "--gpu", "rtxbox-5090", "--for", "10m",
                               "--wait", "5m", "--grace", "1", "--purpose", "T3", "--", "sleep", "600"],
                              env=dict(os.environ, GPUB_CONFIG=str(cfgp)), start_new_session=True,
                              stdout=t3log, stderr=t3log)
        bid3 = None
        for _ in range(90):
            rows = [b for b in api(None, "GET", "/bookings", params={"resource": "rtxbox-5090"}).json() if b["purpose"] == "T3"]
            # wait for the FIRST HEARTBEAT, not just "active": the client heartbeats on its own poll, and killing it
            # before that lands makes the expiry read `no_show` instead of `heartbeat_lost` (flaky). The test's intent
            # is "kill a client that IS heartbeating".
            if rows and rows[0]["state"] == "active" and \
                    (api(None, "GET", f"/bookings/{rows[0]['id']}").json()["lease"] or {}).get("heartbeats", 0) >= 1:
                bid3 = rows[0]["id"]; break
            time.sleep(1)
        check("T3 gpub run reaches active and heartbeats (rtxbox-5090, scratch lockfile taken)", bid3 is not None and LOCK.exists(),
              "" if bid3 else (cfgp.parent / "t3.log").read_text()[-300:])
        if bid3:
            time.sleep(1)
            os.killpg(t3.pid, signal.SIGKILL)          # client AND its child die; nobody heartbeats
            t3.wait()
            t0 = time.time()
            st = wait_state(bid3, {"expired"}, 60 + TICK * 2 + 5)
            elapsed = time.time() - t0
            b = api(None, "GET", f"/bookings/{bid3}").json()
            sigs = [s for s in api(None, "GET", "/signals", params={"limit": 100}).json()
                    if s["type"] == "LEASE_EXPIRED" and s["booking_id"] == bid3]
            delivered = bool(sigs) and "headnode" in sigs[0]["to_machines"] and "*" in sigs[0]["to_machines"]
            check("T3 lease expires after grace, slot released", st == "expired" and b["booking"]["end_reason"] == "heartbeat_lost",
                  f"state={st} reason={b['booking'].get('end_reason')} after {elapsed:.0f}s (grace 60 s + tick {TICK:g} s)")
            check("T3 LEASE_EXPIRED delivered to holder + broadcast", delivered, f"to={sigs[0]['to_machines'] if sigs else None}")
            # the lane is reusable immediately
            r = api(tok_head, "POST", "/bookings/earliest", json={"resource_id": "rtxbox-5090", "mode": "exclusive",
                                                                    "duration_min": 1, "max_wait_min": 0, "grace_min": 0})
            check("T3 freed slot is bookable immediately", r.status_code == 201, r.text[:80])
            check("T3 expiry released the external lockfile", not LOCK.exists() or json.loads(LOCK.read_text())["holder"] != f"gpub:{bid3}")

        # ---------------- T7a: RRULE in a DST zone (expansion half; the job half is in test_agent) ---
        r = api(tok_head, "POST", "/bookings", json={"resource_id": "spark-a-gb10", "mode": "exclusive", "duration_min": 60,
                 "start": "2026-10-27T07:00:00+00:00", "tz": "America/Chicago", "rrule": "FREQ=WEEKLY;BYDAY=TU",
                 "purpose": "T7 weekly 02:00 Chicago", "grace_min": 0})
        check("T7a weekly template accepted", r.status_code == 201 and r.json()["state"] == "template", r.text[:100])
        with psycopg.connect(f"dbname={DB}", row_factory=psycopg.rows.dict_row) as c:
            co.rrule_expand(c, horizon_days=40); c.commit()
            occ = [x["t"].astimezone(dt.UTC) for x in c.execute("SELECT lower(slot) AS t FROM bookings WHERE parent_id=%s ORDER BY 1",
                                              (r.json()["id"],)).fetchall()]
        # rrule_expand materialises from NOW forward, so the expected occurrences are date-relative: the first three
        # Tuesdays 02:00 America/Chicago at or after now (and after the template start), converted to UTC. 02:00 local
        # is 07Z under CDT and 08Z under CST; the expected list therefore crosses a DST change whenever one falls
        # within the next three weeks.
        from zoneinfo import ZoneInfo
        chi = ZoneInfo("America/Chicago"); d = dt.datetime.now(chi).date() - dt.timedelta(days=1); want = []
        while len(want) < 3:
            inst = dt.datetime(d.year, d.month, d.day, 2, tzinfo=chi).astimezone(dt.UTC)
            if d.weekday() == 1 and d >= dt.date(2026, 10, 27) and inst >= dt.datetime.now(dt.UTC) - dt.timedelta(minutes=1):
                want.append(inst)
            d += dt.timedelta(days=1)
        check("T7a weekly 02:00 America/Chicago expands to the DST-correct UTC instants", occ[:3] == want,
              " ".join(o.strftime("%m-%d %H%MZ") for o in occ[:3]))
        rr = api(tok_head, "POST", f"/bookings/{r.json()['id']}/cancel")
        rc = rr.json() if rr.headers.get("content-type", "").startswith("application/json") else {"http": rr.status_code}
        check("T7a cancelling the template cancels its occurrences", rc.get("occurrences_cancelled", 0) >= 3, str(rc))
    finally:
        srv.terminate()
        try: srv.wait(5)
        except subprocess.TimeoutExpired: srv.kill()
        LOCK.unlink(missing_ok=True)

    failed = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed")
    if failed:
        print("GATE: FAIL — " + "; ".join(n for n, _, _ in failed))
        return 1
    print("GATE: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
