#!/usr/bin/env python3
"""test_agent.py — host agent acceptance gate for GPUB. Exits NON-ZERO on any failure.
Runs the host agent on THIS host against a fixture db (gpub_test, DROPPED and recreated), a coordinator on 127.0.0.1:8767,
a FAKE nvidia-smi (script fed from a file) and a FAKE Ollama (local http server on 127.0.0.1:18434) — no GPU, no real
lockfile, no production database touched. tests/run_all.py runs test_phase1.py first.

Covers:  T4 stray process during an exclusive slot -> SQUATTER_DETECTED, nothing killed ·
T6 coordinator unreachable -> the agent starts the job from its cache at slot start; COORDINATOR_DEGRADED + from_cache row
on return · T7b RRULE job executes at the right time on the host · T8 Ollama model unloaded before an exclusive slot.
Invariants: telemetry reaches /status; only the HOST machine may use /agents/<res>/*; public /bookings redacts
job.env values; host agent may heartbeat/report for a booking it does not own; cancel stays owner-only; extension refused
when the next slot is taken (-> SIGTERM once, job 'killed' rc -15) and granted when free (slot grows, job keeps running);
owner release of a running job -> SIGTERM; AGENT_DOWN / AGENT_UP; signal allow-list; GET /jobs.
"""
from __future__ import annotations
import datetime as dt, http.server, json, os, signal, subprocess, sys, tempfile, threading, time
from pathlib import Path

import httpx, psycopg

HERE = Path(__file__).resolve().parent
PKG = HERE.parent / "gpub"
sys.path.insert(0, str(PKG)); sys.path.insert(0, str(HERE))
import coordinator as co  # noqa: E402
import test_phase1 as g1  # noqa: E402   (fresh_db helper)

PY = sys.executable
DB = "gpub_test"            # fixture database: dropped and recreated
PORT = 8767
BIND = "127.0.0.1"
OTHER_IP = "127.0.0.2"
RES = "spark-a-gb10"        # the fixture resource this host's agent plays; also the pkill pattern at cleanup
URL = f"http://{BIND}:{PORT}"
TICK = 2.0
POLL = 2.0
OLLAMA_PORT = 18434
STALE_S = 8.0
results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""), flush=True)


def api(tok, method, path, **kw):
    h = {"Authorization": f"Bearer {tok}"} if tok else {}
    return httpx.request(method, URL + path, headers=h, timeout=20, **kw)


def wait_for(pred, timeout: float, step: float = 1.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            v = pred()
        except Exception:
            v = None
        if v:
            return v
        time.sleep(step)
    return None


def sigs(type_: str, bid: int | None = None) -> list[dict]:
    rows = api(None, "GET", "/signals", params={"limit": 300}).json()
    return [s for s in rows if s["type"] == type_ and (bid is None or s["booking_id"] == bid)]


def booking(bid: int) -> dict:
    return api(None, "GET", f"/bookings/{bid}").json()


# ------------------------------------------------------------------ fakes
class FakeOllama(http.server.BaseHTTPRequestHandler):
    state = {"loaded": ["qwen3.8:27b"], "unloads": []}

    def log_message(self, *a): pass

    def _send(self, code, obj):
        b = json.dumps(obj).encode(); self.send_response(code); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)

    def do_GET(self):
        if self.path == "/api/ps":
            return self._send(200, {"models": [{"name": n, "model": n, "size": 17_000_000_000, "size_vram": 17_000_000_000}
                                               for n in self.state["loaded"]]})
        self._send(404, {})

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0)); body = json.loads(self.rfile.read(n) or b"{}")
        if self.path == "/api/generate" and body.get("keep_alive") == 0:
            self.state["unloads"].append(body.get("model"))
            if body.get("model") in self.state["loaded"]:
                self.state["loaded"].remove(body["model"])
            return self._send(200, {"model": body.get("model"), "done": True, "done_reason": "unload"})
        self._send(400, {"error": "unexpected"})


def write_fake_smi(path: Path, apps_file: Path) -> None:
    path.write_text(f"""#!/usr/bin/env bash
# fake nvidia-smi for the gpub gate: GB10-shaped output (memory N/A), compute apps from a file
case "$*" in
  *query-gpu*) echo "7, 41, [N/A], [N/A], 12.50" ;;
  *query-compute-apps*) [ -f "{apps_file}" ] && cat "{apps_file}" ;;
  *) echo "fake" ;;
esac
""")
    path.chmod(0o755)


# ------------------------------------------------------------------ part 2
def phase2() -> None:
    g1.fresh_db()                                   # drops/recreates gpub_test; applies schema + migrations via co
    tok_head = co.register(DB, "headnode", BIND, "linux-wsl2")
    tok_rtx = co.register(DB, "rtxbox", OTHER_IP, "linux-wsl2")
    with psycopg.connect(f"dbname={DB}", autocommit=True) as c:
        c.execute("UPDATE resources SET host_machine='headnode' WHERE resource_id=%s", (RES,))   # fixture: this host plays spark-a
    scratch = Path(tempfile.mkdtemp(prefix="gpub_agent_test_"))
    apps_file = scratch / "smi_apps"; smi = scratch / "nvidia-smi"; write_fake_smi(smi, apps_file)
    cfgp = scratch / "config.json"; cfgp.write_text(json.dumps({"url": URL, "machine": "headnode", "token": tok_head})); cfgp.chmod(0o600)
    state_dir = scratch / "state"; log_dir = scratch / "joblogs"
    env = dict(os.environ, GPUB_DB=DB, GPUB_AGENT_STALE_S=str(STALE_S))
    srv = subprocess.Popen([PY, str(PKG / "coordinator.py"), "--db", DB, "serve", "--bind", BIND, "--port", str(PORT),
                            "--tick", str(TICK)], env=env, stdout=open(scratch / "co.log", "w"), stderr=subprocess.STDOUT)
    ol = http.server.ThreadingHTTPServer(("127.0.0.1", OLLAMA_PORT), FakeOllama)
    threading.Thread(target=ol.serve_forever, daemon=True).start()
    agent_log = open(scratch / "agent.log", "w")

    def start_agent():
        return subprocess.Popen([PY, str(PKG / "agent.py"), "--resource", RES, "--poll", str(POLL),
                                 "--state-dir", str(state_dir), "--nvidia-smi", str(smi), "--ollama", f"http://127.0.0.1:{OLLAMA_PORT}",
                                 "--log-dir", str(log_dir), "--timeout", "3", "--unload-lead", "30", "--extend-lead", "40",
                                 "--extend-min", "15"], env=dict(os.environ, GPUB_CONFIG=str(cfgp)), stdout=agent_log, stderr=agent_log)
    agent = None; squat = None
    try:
        ok = wait_for(lambda: httpx.get(URL + "/time", timeout=2).status_code == 200, 10, 0.25)
        check("P2 coordinator starts", bool(ok), URL)
        if not ok: return
        agent = start_agent()
        # ---- telemetry + identity
        st = wait_for(lambda: (lambda s: s if s["gpu"] and s["agent"] else None)(
            [r for r in api(None, "GET", "/status").json()["resources"] if r["resource"]["resource_id"] == RES][0]), 15)
        check("P2 telemetry reaches /status (fake GB10: util 7, mem N/A -> null)",
              bool(st) and st["gpu"].get("util_pct") == 7 and st["gpu"].get("mem_used_gb") is None and st["agent"].get("version") == "gpub-agent 1.0",
              json.dumps(st and {k: st["gpu"].get(k) for k in ("util_pct", "temp_c", "mem_used_gb", "power_w")}))
        r = api(tok_head, "GET", "/agents/rtxbox-5090/plan")
        check("P2 /agents/<res>/plan refused for a non-host machine (403)", r.status_code == 403, r.text[:100])
        r = api(tok_head, "GET", f"/agents/{RES}/plan")
        check("P2 /agents/<res>/plan served to the host (resource row + bookings + clock)",
              r.status_code == 200 and r.json()["resource"]["resource_id"] == RES and "utc" in r.json(), r.text[:80])
        r = api(tok_head, "POST", "/signals", json={"type": "BOGUS", "severity": "yellow", "message": "x"})
        check("P2 POST /signals rejects a type outside the allow-list (422)", r.status_code == 422, r.text[:80])
        # ---- redaction
        far = dt.datetime(2031, 5, 5, 12, tzinfo=dt.UTC)
        r = api(tok_head, "POST", "/bookings", json={"resource_id": RES, "mode": "exclusive", "duration_min": 30,
                 "start": far.isoformat(), "purpose": "redact", "job": {"cmd": "echo x", "env": {"SECRET": "abc123"}}})
        rid_ = r.json()["id"]
        pub = [b for b in api(None, "GET", "/bookings", params={"to": (far + dt.timedelta(days=1)).isoformat(), "resource": RES}).json() if b["id"] == rid_][0]
        plan = [b for b in api(tok_head, "GET", f"/agents/{RES}/plan", params={"days": 3000}).json()["bookings"] if b["id"] == rid_][0]
        check("P2 public /bookings redacts job.env values; host plan carries them",
              pub["job"]["env"]["SECRET"] == "<redacted>" and plan["job"]["env"]["SECRET"] == "abc123", f"public={pub['job']['env']} plan={plan['job']['env']}")
        api(tok_head, "POST", f"/bookings/{rid_}/cancel")

        # ---- A: owned by RTXBOX (raw SQL), hosted by this agent -> T8 unload, job start, host heartbeat, T4, refused extension -> TERM
        now = dt.datetime.now(dt.UTC).replace(microsecond=0)
        a_end = now + dt.timedelta(seconds=120)
        FakeOllama.state["loaded"] = ["qwen3.8:27b"]; FakeOllama.state["unloads"] = []
        with psycopg.connect(f"dbname={DB}", autocommit=True) as c:
            A = c.execute("""INSERT INTO bookings (resource_id, owner_machine, mode, slot, purpose, grace_min, job)
                             VALUES (%s,'rtxbox','exclusive', tstzrange(%s,%s,'[)'), 'P2-A', 1,
                                     '{"cmd": "echo hello; sleep 500"}'::jsonb) RETURNING id""", (RES, now, a_end)).fetchone()[0]
        ok = wait_for(lambda: FakeOllama.state["unloads"] and sigs("OLLAMA_UNLOADED", A), 20)
        check("T8 Ollama model unloaded before the exclusive slot (keep_alive 0) + OLLAMA_UNLOADED",
              bool(ok) and FakeOllama.state["loaded"] == [], f"unloads={FakeOllama.state['unloads']} loaded={FakeOllama.state['loaded']}")
        jb = wait_for(lambda: (lambda b: b if b["job"] and b["job"]["state"] == "running" else None)(booking(A)), 20)
        check("P2 job starts on the host when the slot goes active (JOB_STARTED, jobs row running)",
              bool(jb) and bool(sigs("JOB_STARTED", A)), json.dumps(jb and {k: jb["job"][k] for k in ("state", "pid", "from_cache")}))
        jpid = jb["job"]["pid"] if jb else None
        hb = wait_for(lambda: (lambda b: b if (b["lease"] or {}).get("heartbeats", 0) >= 2 else None)(booking(A)), 15)
        check("P2 host agent heartbeats a booking it does NOT own (owner rtxbox, host headnode)", bool(hb),
              f"heartbeats={(hb or booking(A))['lease'] and (hb or booking(A))['lease'].get('heartbeats')}")
        r = api(tok_head, "POST", f"/bookings/{A}/cancel")
        check("P2 cancel stays owner-only (host gets 403)", r.status_code in (403, 409), f"{r.status_code} {r.text[:80]}")
        # T4 squatter
        squat = subprocess.Popen(["sleep", "400"], start_new_session=True)
        apps_file.write_text(f"{squat.pid}, squat_proc, 100\n{jpid}, bash, 50\n")
        sq = wait_for(lambda: sigs("SQUATTER_DETECTED", A), 15)
        mine_flagged = [s for s in sigs("SQUATTER_DETECTED", A) if f"pid {jpid} " in s["message"]]
        check("T4 stray compute process during an exclusive slot -> SQUATTER_DETECTED (yellow), the job itself not flagged",
              bool(sq) and f"pid {squat.pid} " in sq[0]["message"] and not mine_flagged and sq[0]["severity"] == "yellow", (sq or [{}])[0].get("message", "")[:120])
        time.sleep(POLL + 1)
        check("T4 nothing was killed (squatter still alive; soft enforcement)", squat.poll() is None)
        apps_file.unlink(missing_ok=True)
        # B right after A -> A's extension must be refused, A's job SIGTERMed at slot end
        r = api(tok_head, "POST", "/bookings", json={"resource_id": RES, "mode": "exclusive", "duration_min": 1,
                 "start": a_end.isoformat(), "purpose": "P2-B", "grace_min": 0, "job": {"cmd": "sleep 300"}})
        check("P2 back-to-back booking B accepted", r.status_code == 201, r.text[:100]); B = r.json()["id"]
        remaining = (a_end - dt.datetime.now(dt.UTC)).total_seconds()
        ja = wait_for(lambda: (lambda b: b if b["job"]["state"] != "running" else None)(booking(A)), remaining + TICK * 2 + POLL * 3 + 5)
        check("P2 extension refused when the next slot is taken (no SLOT_EXTENDED for A)", not sigs("SLOT_EXTENDED", A))
        check("P2 slot end with a waiting neighbour -> job SIGTERMed once (JOB_SLOT_END_TERM), jobs row killed rc=-15",
              bool(ja) and ja["job"]["state"] == "killed" and ja["job"]["exit_code"] == -15 and bool(sigs("JOB_SLOT_END_TERM", A)),
              json.dumps(ja and {k: ja["job"][k] for k in ("state", "exit_code")}))
        check("P2 A ended at slot end (released by coordinator), B active", booking(A)["booking"]["state"] == "released"
              and bool(wait_for(lambda: booking(B)["booking"]["state"] == "active", TICK * 2 + 2)), f"A={booking(A)['booking']['state']} B={booking(B)['booking']['state']}")
        # ---- B: nothing after it -> extension granted, job keeps running past the original end; owner release -> TERM
        b_end0 = dt.datetime.fromisoformat(booking(B)["booking"]["end"])
        ex = wait_for(lambda: sigs("SLOT_EXTENDED", B), (b_end0 - dt.datetime.now(dt.UTC)).total_seconds() + 10)
        bb = booking(B)["booking"]
        check("P2 extension granted when the time is free (+15 min, extended_min=15, SLOT_EXTENDED)",
              bool(ex) and dt.datetime.fromisoformat(bb["end"]) == b_end0 + dt.timedelta(minutes=15) and bb["extended_min"] == 15,
              f"end {b_end0:%H:%M:%S} -> {bb['end'][11:19]} extended_min={bb['extended_min']}")
        time.sleep(max(0, (b_end0 - dt.datetime.now(dt.UTC)).total_seconds()) + TICK + 2)
        bj = booking(B)
        check("P2 job runs past its original slot end once extended (still active, job running)",
              bj["booking"]["state"] == "active" and bj["job"]["state"] == "running", f"{bj['booking']['state']} / {bj['job']['state']}")
        r = api(tok_head, "POST", f"/bookings/{B}/release")
        jb2 = wait_for(lambda: (lambda b: b if b["job"]["state"] != "running" else None)(booking(B)), POLL * 3 + 5)
        check("P2 owner release of a running job -> SIGTERM, jobs row killed", r.status_code == 200 and bool(jb2) and jb2["job"]["state"] == "killed",
              json.dumps(jb2 and jb2["job"]["state"]))
        # ---- T7b: RRULE template -> occurrence -> job runs at the right time; rc 0 -> JOB_EXITED + released early
        f_start = (dt.datetime.now(dt.UTC) + dt.timedelta(seconds=25)).replace(microsecond=0)
        r = api(tok_head, "POST", "/bookings", json={"resource_id": RES, "mode": "exclusive", "duration_min": 1,
                 "start": f_start.isoformat(), "tz": "America/Chicago", "rrule": "FREQ=DAILY;COUNT=1", "purpose": "P2-F", "grace_min": 0,
                 "job": {"cmd": "date -u +%s > $GPUB_STATE/f_ran", "env": {"GPUB_STATE": str(scratch)}}})
        check("T7b RRULE job template accepted", r.status_code == 201 and r.json()["state"] == "template", r.text[:100]); F = r.json()["id"]
        occ = wait_for(lambda: [b for b in api(None, "GET", "/bookings", params={"resource": RES}).json() if b.get("parent_id") == F], TICK * 2 + 2)
        check("T7b occurrence materialised from the template", bool(occ), str(occ and occ[0]["start"]))
        ran = wait_for(lambda: (scratch / "f_ran").exists() and (scratch / "f_ran").read_text().strip(), 25 + TICK + POLL + 8)
        delay = (int(ran) - f_start.timestamp()) if ran else None
        check("T7b job executed on the host within tick+poll of the scheduled start", ran is not None and 0 <= delay <= TICK + POLL + 3,
              f"delay {delay} s (tick {TICK:g} + poll {POLL:g})")
        Fo = occ[0]["id"] if occ else -1
        fin = wait_for(lambda: (lambda b: b if b["booking"]["state"] == "released" else None)(booking(Fo)), POLL * 3 + TICK + 5)
        check("T7b rc 0 -> JOB_EXITED, slot released early by the host agent (release_on_exit)",
              bool(fin) and fin["booking"]["end_reason"] == "holder_release" and fin["job"]["state"] == "exited" and bool(sigs("JOB_EXITED", Fo)),
              json.dumps(fin and {"state": fin["booking"]["state"], "reason": fin["booking"]["end_reason"], "job": fin["job"]["state"]}))
        copied = list((log_dir / RES).glob("*.log")) if log_dir.exists() else []
        check("P2 job logs copied to --log-dir on exit", len(copied) >= 3, f"{len(copied)} files")
        # ---- T6: coordinator unreachable -> the agent runs the job from its cache
        e_start = (dt.datetime.now(dt.UTC) + dt.timedelta(seconds=30)).replace(microsecond=0)
        r = api(tok_head, "POST", "/bookings", json={"resource_id": RES, "mode": "exclusive", "duration_min": 2,
                 "start": e_start.isoformat(), "purpose": "P2-E", "grace_min": 0,
                 "job": {"cmd": "touch $GPUB_STATE/e_ran; sleep 20", "env": {"GPUB_STATE": str(scratch)}}})
        check("T6 booking E accepted", r.status_code == 201, r.text[:100]); E = r.json()["id"]
        time.sleep(POLL * 2 + 1)                      # let the agent cache the plan with E in it
        cache = json.loads((state_dir / f"plan_{RES}.json").read_text())
        check("T6 agent cache holds E with its full job", any(b["id"] == E and b["job"] for b in cache["bookings"]))
        os.kill(srv.pid, signal.SIGSTOP)
        try:
            httpx.get(URL + "/time", timeout=2); frozen = False
        except httpx.HTTPError:
            frozen = True
        check("T6 coordinator frozen (SIGSTOP): /time does not answer", frozen)
        ran_e = wait_for(lambda: (scratch / "e_ran").exists(), (e_start - dt.datetime.now(dt.UTC)).total_seconds() + POLL * 3 + 5)
        check("T6 job started FROM CACHE at slot start while the coordinator was unreachable", bool(ran_e))
        os.kill(srv.pid, signal.SIGCONT)
        deg = wait_for(lambda: [s for s in sigs("COORDINATOR_DEGRADED") if f"#{E}" in s["message"]], POLL * 4 + 10)
        check("T6 COORDINATOR_DEGRADED emitted on return, naming the cache-started job", bool(deg), (deg or [{}])[0].get("message", "")[:140])
        je = wait_for(lambda: (lambda b: b if b["job"] else None)(booking(E)), POLL * 3 + 5)
        check("T6 queued JOB_STARTED replayed: jobs row from_cache=true", bool(je) and je["job"]["from_cache"] is True, json.dumps(je and je["job"]["from_cache"]))
        fe = wait_for(lambda: (lambda b: b if b["job"]["state"] != "running" else None)(booking(E)), 30 + POLL * 3)
        check("T6 cache-started job completes and is recorded (exited rc 0)", bool(fe) and fe["job"]["state"] == "exited" and fe["job"]["exit_code"] == 0,
              json.dumps(fe and {k: fe["job"][k] for k in ("state", "exit_code")}))
        # ---- AGENT_DOWN / AGENT_UP
        agent.terminate(); agent.wait(10)
        dn = wait_for(lambda: sigs("AGENT_DOWN"), STALE_S + TICK * 2 + 5)
        check("P2 agent silent -> AGENT_DOWN (yellow) from the coordinator", bool(dn) and dn[0]["severity"] == "yellow", (dn or [{}])[0].get("message", "")[:100])
        agent = start_agent()
        up = wait_for(lambda: sigs("AGENT_UP"), POLL * 3 + TICK * 2 + 5)
        check("P2 agent returns -> AGENT_UP (green)", bool(up), (up or [{}])[0].get("message", "")[:100])
        rows = api(None, "GET", "/jobs", params={"resource": RES}).json()
        check("P2 GET /jobs lists the job rows", len(rows) >= 4 and {r_["state"] for r_ in rows} >= {"killed", "exited"}, f"{len(rows)} rows: {[r_['state'] for r_ in rows]}")
        rep = httpx.get(URL + "/report", timeout=20).text
        check("P2 /report carries agent telemetry + jobs summary", "agent headnode gpub-agent 1.0" in rep and "jobs:" in rep, rep[:0])
    finally:
        for p in (agent, squat):
            if p and p.poll() is None:
                p.terminate()
                try: p.wait(5)
                except subprocess.TimeoutExpired: p.kill()
        try: os.kill(srv.pid, signal.SIGCONT)
        except Exception: pass
        srv.terminate()
        try: srv.wait(5)
        except subprocess.TimeoutExpired: srv.kill()
        ol.shutdown()
        # leftover job children (sleep 300/500 got SIGTERM; make sure nothing of the fixture lingers)
        # (the pattern names only the fixture resource id, so it cannot match anything outside this test)
        subprocess.run(["pkill", "-TERM", "-f", f"GPUB_RESOURCE={RES}"], check=False)
        print(f"fixture dir kept for inspection: {scratch}")


def main() -> int:
    print("=== test_agent: host agent (fake nvidia-smi + fake Ollama) ===", flush=True)
    phase2()
    failed = [r for r in results if not r[1]]
    print(f"\nagent: {len(results) - len(failed)}/{len(results)} checks passed")
    if failed:
        print("GATE: FAIL — " + "; ".join(n for n, _, _ in failed))
        return 1
    print("GATE: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
