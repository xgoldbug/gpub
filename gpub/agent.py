#!/usr/bin/env python3
"""agent.py — GPUB (GPU Booker) HOST AGENT. Runs on the machine that owns a GPU (one agent per resource).

  agent.py --resource spark-a-gb10 [--poll 30] [--state-dir ~/.local/state/gpub]
           [--nvidia-smi PATH] [--ollama http://127.0.0.1:11434] [--log-dir ~/.local/state/gpub/jobs]
           [--timeout 8] [--unload-lead 90] [--extend-lead 60] [--extend-min 15] [--once] [--dry]

What it does, every --poll seconds (stdlib only: no pip on the host):
  1. SAMPLE  nvidia-smi fields that exist (GB10 reports memory N/A -> null; no nvidia-smi -> gpu.error) + local Ollama /api/ps.
  2. PLAN    GET /agents/<resource>/plan (7 days, full job payloads) -> written VERBATIM to the local cache.
             Coordinator unreachable -> run from the cache (reconcile when it returns: COORDINATOR_DEGRADED, queued reports).
  3. REPORT  POST /agents/<resource>/report (telemetry).  The coordinator flags AGENT_DOWN when this stops.
  4. ACT     - exclusive slot within --unload-lead: ask local Ollama to unload every loaded model (keep_alive 0). OLLAMA_UNLOADED.
             - booking ACTIVE with a job: start it HERE (localhost:11434 therefore means THIS GPU; zero bot edits).
               Log to <state>/jobs/<id>.log, copy to --log-dir on exit. Heartbeat for it. JOB_STARTED / JOB_EXITED / JOB_FAILED.
             - job still running near slot end: POST /bookings/<id>/extend (+--extend-min). The DB decides; a 409 means
               someone else holds that time. Refused or slot gone -> SIGTERM to the job's own process group, ONCE
               (job.on_slot_end = "continue" disables even that). Never SIGKILL. JOB_SLOT_END_TERM / JOB_OVERRUN.
             - exclusive slot active and a compute process outside the job's session appears -> SQUATTER_DETECTED (yellow).
               NOTHING IS KILLED (soft enforcement). Ollama's own processes are never squatters (shared engine).
             - coordinator down: a job whose slot has begun starts from the cache (resource without an external lockfile,
               or the agent takes that lockfile itself as gpub:<id>). Clock skew > 5 s -> no cache-driven starts (CLOCK_SKEW).
  5. SAVE    state (<state>/agent_<resource>.json) so a restart reconciles running jobs instead of double-starting.

Identity: ~/.config/gpub/config.json {"url","machine","token"} (GPUB_CONFIG overrides). The coordinator also requires the
request to come from this machine's registered IP, and only the resource's HOST machine may use /agents/<resource>/*.
"""
import argparse, datetime as dt, json, logging, os, shutil, signal, socket, subprocess, sys, time, urllib.error, urllib.request
from pathlib import Path
from typing import Any, Optional

VERSION = "gpub-agent 1.0"
LOCK_MAX_HOLD_H = 6.0          # stale-lock age for the optional external lockfile (same rule as the coordinator)
SKEW_LIMIT_S = 5.0
log = logging.getLogger("gpub-agent")


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def iso(t: Optional[dt.datetime]) -> Optional[str]:
    return t.isoformat() if t else None


def parse(t: str) -> dt.datetime:
    return dt.datetime.fromisoformat(t)


# ------------------------------------------------------------------ coordinator client (urllib, no deps)
class Coordinator:
    def __init__(self, url: str, token: str, timeout: float):
        self.url, self.token, self.timeout = url.rstrip("/"), token, timeout

    def call(self, method: str, path: str, body: Optional[dict] = None, auth: bool = True) -> tuple[Optional[int], Any]:
        """(status, json) ; (None, error-string) when the coordinator is unreachable."""
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.url + path, data=data, method=method)
        req.add_header("Content-Type", "application/json")
        if auth:
            req.add_header("Authorization", f"Bearer {self.token}")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                raw = r.read()
                return r.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as e:
            try:
                return e.code, json.loads(e.read())
            except Exception:
                return e.code, {"detail": str(e)}
        except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError, OSError) as e:
            return None, f"{e.__class__.__name__}: {e}"


# ------------------------------------------------------------------ sampling
def find_nvidia_smi(explicit: Optional[str]) -> Optional[str]:
    for cand in ([explicit] if explicit else []) + [shutil.which("nvidia-smi"), "/usr/lib/wsl/lib/nvidia-smi", "/usr/bin/nvidia-smi"]:
        if cand and Path(cand).exists():
            return cand
    return None


def _num(s: str) -> Optional[float]:
    s = s.strip()
    if not s or s.upper() in ("[N/A]", "N/A", "[NOT SUPPORTED]", "[UNKNOWN ERROR]"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def sample_gpu(smi: Optional[str]) -> dict:
    if not smi:
        return {"error": "nvidia-smi not found"}
    out: dict[str, Any] = {}
    try:
        q = subprocess.run([smi, "--query-gpu=utilization.gpu,temperature.gpu,memory.used,memory.total,power.draw",
                            "--format=csv,noheader,nounits"], capture_output=True, text=True, timeout=15)
        line = (q.stdout.strip().splitlines() or [""])[0]
        parts = [p for p in line.split(",")] if line else []
        if len(parts) >= 5:
            mu, mt = _num(parts[2]), _num(parts[3])
            out |= {"util_pct": _num(parts[0]), "temp_c": _num(parts[1]),
                    "mem_used_gb": round(mu / 1024, 1) if mu is not None else None,
                    "mem_total_gb": round(mt / 1024, 1) if mt is not None else None, "power_w": _num(parts[4])}
        else:
            out["error"] = (q.stderr.strip() or q.stdout.strip())[:200] or f"rc={q.returncode}"
        a = subprocess.run([smi, "--query-compute-apps=pid,process_name,used_memory", "--format=csv,noheader,nounits"],
                           capture_output=True, text=True, timeout=15)
        apps = []
        for ln in a.stdout.strip().splitlines():
            cols = [c.strip() for c in ln.split(",")]
            if len(cols) >= 2 and cols[0].isdigit():
                apps.append({"pid": int(cols[0]), "name": cols[1][:80], "mem_mb": _num(cols[2]) if len(cols) > 2 else None})
        out["apps"] = apps
    except (subprocess.TimeoutExpired, OSError) as e:
        out["error"] = f"{e.__class__.__name__}: {e}"[:200]
    return out


def ollama_ps(base: str, timeout: float) -> dict:
    try:
        with urllib.request.urlopen(base.rstrip("/") + "/api/ps", timeout=timeout) as r:
            d = json.loads(r.read() or b"{}")
        loaded = [{"name": m.get("name") or m.get("model"), "size_gb": round((m.get("size_vram") or m.get("size") or 0) / 1e9, 1)}
                  for m in d.get("models", [])]
        return {"reachable": True, "loaded": loaded}
    except Exception as e:
        return {"reachable": False, "loaded": [], "error": f"{e.__class__.__name__}"[:60]}


def ollama_unload(base: str, name: str, timeout: float) -> bool:
    try:
        req = urllib.request.Request(base.rstrip("/") + "/api/generate", data=json.dumps({"model": name, "keep_alive": 0}).encode(),
                                     method="POST", headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=max(timeout, 30)) as r:
            r.read()
        return True
    except Exception as e:
        log.warning("ollama unload %s failed: %s", name, e)
        return False


# ------------------------------------------------------------------ optional external lockfile (agent-side, cache mode only)
def lock_read(path: str) -> Optional[dict]:
    try:
        return json.loads(Path(path).read_text())
    except Exception:
        return None


def lock_acquire(path: str, holder: str, note: str) -> tuple[bool, Optional[dict]]:
    info = lock_read(path)
    if info and info.get("holder") == holder:
        return True, None
    if info:
        try:
            age_h = (dt.datetime.now() - dt.datetime.fromisoformat(info["started"])).total_seconds() / 3600
        except Exception:
            age_h = 1e9
        if age_h > LOCK_MAX_HOLD_H:
            log.warning("external lockfile: reclaiming STALE lock from %r (%.1f h)", info.get("holder"), age_h)
            try: os.remove(path)
            except FileNotFoundError: pass
        else:
            return False, info
    payload = json.dumps({"holder": holder, "host": socket.gethostname(), "pid": os.getpid(), "pid_from": "in-process",
                          "started": dt.datetime.now().isoformat(timespec="seconds"), "note": note})
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644); os.write(fd, payload.encode()); os.close(fd)
        return True, None
    except FileExistsError:
        return False, lock_read(path)
    except OSError as e:
        return False, {"holder": f"<unwritable: {e}>"}


def lock_release(path: str, holder: str) -> None:
    info = lock_read(path)
    if info and info.get("holder") == holder:
        try: os.remove(path)
        except FileNotFoundError: pass


# ------------------------------------------------------------------ the agent
class Agent:
    def __init__(self, a: argparse.Namespace):
        self.a = a
        cfgp = Path(os.environ.get("GPUB_CONFIG", Path.home() / ".config" / "gpub" / "config.json"))
        cfg = json.loads(cfgp.read_text())
        self.machine = cfg["machine"]
        self.co = Coordinator(cfg["url"], cfg["token"], a.timeout)
        self.rid = a.resource
        self.state_dir = Path(a.state_dir).expanduser(); (self.state_dir / "jobs").mkdir(parents=True, exist_ok=True)
        self.state_path = self.state_dir / f"agent_{self.rid}.json"
        self.cache_path = self.state_dir / f"plan_{self.rid}.json"
        self.smi = find_nvidia_smi(a.nvidia_smi)
        self.started = utcnow()
        self.procs: dict[int, subprocess.Popen] = {}
        self.online: Optional[bool] = None
        self.offline_since: Optional[dt.datetime] = None
        self.skew_s = 0.0
        self.skew_bad = False
        self.st = self._load_state()

    # ---- persistence
    def _load_state(self) -> dict:
        try:
            s = json.loads(self.state_path.read_text())
        except Exception:
            s = {}
        s.setdefault("jobs", {}); s.setdefault("pending", []); s.setdefault("squatters", []); s.setdefault("done", [])
        s.setdefault("unloaded_for", []); s.setdefault("cache_runs", [])
        return s

    def save(self) -> None:
        tmp = self.state_path.with_suffix(".tmp"); tmp.write_text(json.dumps(self.st, indent=1, default=str)); tmp.replace(self.state_path)

    def load_cache(self) -> Optional[dict]:
        try:
            return json.loads(self.cache_path.read_text())
        except Exception:
            return None

    # ---- coordinator helpers
    def send(self, method: str, path: str, body: Optional[dict] = None, queue_if_offline: bool = True) -> tuple[Optional[int], Any]:
        if self.a.dry and method != "GET":
            log.info("DRY %s %s %s", method, path, json.dumps(body)[:200] if body else ""); return 200, {"dry": True}
        st, js = self.co.call(method, path, body)
        if st is None and queue_if_offline:
            self.st["pending"].append({"method": method, "path": path, "body": body, "ts": iso(utcnow())})
            log.warning("queued %s %s (coordinator unreachable)", method, path)
        return st, js

    def signal(self, type_: str, severity: str, message: str, booking_id: Optional[int] = None, to: Optional[list[str]] = None) -> None:
        log.info("signal %s %s: %s", severity.upper(), type_, message)
        self.send("POST", "/signals", {"type": type_, "severity": severity, "message": message, "resource_id": self.rid,
                                      "booking_id": booking_id, "to": to})

    def flush_pending(self) -> None:
        """Replay queued reports in order. Stop at the first connection failure (keep it and everything after it);
        a 4xx is logged and dropped so one bad report cannot wedge the queue."""
        q = self.st["pending"]
        for i, p in enumerate(q):
            st, js = self.co.call(p["method"], p["path"], p["body"])
            if st is None:
                self.st["pending"] = q[i:]; return
            if st >= 400:
                log.warning("pending %s %s dropped: %s %s", p["method"], p["path"], st, str(js)[:120])
        self.st["pending"] = []

    def check_skew(self) -> None:
        t0 = time.time(); st, js = self.co.call("GET", "/time", auth=False); rtt = time.time() - t0
        if st == 200:
            srv = parse(js["utc"]); self.skew_s = (srv - utcnow()).total_seconds() + rtt / 2
            bad = abs(self.skew_s) > SKEW_LIMIT_S
            if bad and not self.skew_bad:
                self.signal("CLOCK_SKEW", "yellow", f"{self.machine} clock skew {self.skew_s:+.1f} s vs coordinator; "
                            f"cache-driven slot starts disabled on {self.rid} until fixed")
            self.skew_bad = bad

    # ---- plan
    def fetch_plan(self) -> tuple[Optional[dict], bool]:
        st, js = self.co.call("GET", f"/agents/{self.rid}/plan")
        if st == 200:
            js["fetched_at"] = iso(utcnow())
            tmp = self.cache_path.with_suffix(".tmp"); tmp.write_text(json.dumps(js, indent=1)); tmp.replace(self.cache_path)
            return js, True
        if st is not None:
            log.error("plan refused: %s %s — check token/host identity", st, str(js)[:200])
            return self.load_cache(), False      # reachable but refusing us: behave as offline, never invent bookings
        return self.load_cache(), False

    def on_transition(self, online: bool) -> None:
        now = utcnow()
        if online and self.online is False:
            out = (now - self.offline_since).total_seconds() if self.offline_since else 0
            runs = [r for r in self.st["cache_runs"]]
            self.flush_pending()
            if out >= 3 * self.a.poll or runs:
                self.signal("COORDINATOR_DEGRADED", "yellow",
                            f"coordinator unreachable from {self.machine} for {out/60:.1f} min (since {iso(self.offline_since)}); "
                            f"{self.rid} ran from its local cache; jobs started from cache: "
                            + (", ".join(f"#{r}" for r in runs) if runs else "none"))
            self.st["cache_runs"] = []
            self.offline_since = None
            self.check_skew()
        elif not online and self.online is not False:
            self.offline_since = now
            log.warning("coordinator unreachable; acting from cache %s", self.cache_path)
        self.online = online

    # ---- jobs
    def start_job(self, b: dict, from_cache: bool, resource: dict) -> None:
        bid = b["id"]; job = b["job"] or {}
        cmd = job.get("cmd")
        if not cmd:
            return
        if from_cache and resource.get("gpu_lock_path"):
            ok, blocker = lock_acquire(resource["gpu_lock_path"], f"gpub:{bid}", f"gpub agent cache-start booking {bid}")
            if not ok:
                self.signal("LANE_BLOCKED", "yellow", f"#{bid} not started from cache: lockfile {resource['gpu_lock_path']} held by "
                            f"{(blocker or {}).get('holder')} on {(blocker or {}).get('host')}", booking_id=bid)
                return
        logp = self.state_dir / "jobs" / f"{bid}.log"
        env = dict(os.environ)
        env["PATH"] = f"{Path.home()}/.local/bin:" + env.get("PATH", "")
        for k, v in (job.get("env") or {}).items():
            env[str(k)] = str(v)
        env |= {"GPUB_BOOKING_ID": str(bid), "GPUB_RESOURCE": self.rid, "GPUB_SLOT_START": b["start"], "GPUB_SLOT_END": b["end"],
                "GPUB_OWNER": b["owner_machine"], "GPUB_FROM_CACHE": "1" if from_cache else "0"}
        cwd = job.get("cwd") or str(Path.home())
        argv = ["bash", "-c", cmd] if isinstance(cmd, str) else [str(x) for x in cmd]
        if self.a.dry:
            log.info("DRY start job #%s: %s", bid, argv); return
        try:
            fh = open(logp, "a")
            fh.write(f"# gpub job for booking #{bid} on {self.rid} ({self.machine}) started {iso(utcnow())}"
                     f"{' FROM CACHE' if from_cache else ''}; slot {b['start']} -> {b['end']}; owner {b['owner_machine']}; "
                     f"purpose {b.get('purpose','')}\n# cmd: {cmd}\n"); fh.flush()
            p = subprocess.Popen(argv, cwd=cwd if Path(cwd).is_dir() else str(Path.home()), env=env, stdout=fh, stderr=subprocess.STDOUT,
                                 stdin=subprocess.DEVNULL, start_new_session=True)
        except OSError as e:
            self.signal("AGENT_NOTE", "yellow", f"#{bid} job could not start on {self.rid}: {e}", booking_id=bid)
            return
        self.procs[bid] = p
        self.st["jobs"][str(bid)] = {"pid": p.pid, "started_at": iso(utcnow()), "log": str(logp), "from_cache": from_cache,
                                     "cmd": cmd if isinstance(cmd, str) else json.dumps(cmd), "cwd": cwd,
                                     "snapshot": {k: b.get(k) for k in ("id", "start", "end", "owner_machine", "mode", "state", "purpose", "job")},
                                     "term_sent_at": None, "overrun_flagged": False, "extend_refused_for": None,
                                     "cache_extended_until": None, "lock_path": resource.get("gpu_lock_path") if from_cache else None}
        if from_cache:
            self.st["cache_runs"].append(bid)
        log.info("job #%s started pid %s%s: %s", bid, p.pid, " FROM CACHE" if from_cache else "", cmd)
        self.send("POST", f"/bookings/{bid}/job", {"event": "started", "pid": p.pid, "log_path": str(logp), "from_cache": from_cache,
                                                   "cmd": self.st["jobs"][str(bid)]["cmd"], "cwd": cwd, "run_user": os.environ.get("USER"),
                                                   "started_at": self.st["jobs"][str(bid)]["started_at"]})

    def _alive(self, bid: int, j: dict) -> tuple[bool, Optional[int]]:
        p = self.procs.get(bid)
        if p is not None:
            rc = p.poll()
            return rc is None, rc
        try:                                   # reconciled after a restart: not our child; liveness only
            os.kill(j["pid"], 0); return True, None
        except ProcessLookupError:
            return False, None
        except PermissionError:
            return True, None

    def _term(self, bid: int, j: dict, why: str) -> None:
        if j.get("term_sent_at"):
            return
        policy = ((j.get("snapshot") or {}).get("job") or {}).get("on_slot_end", "term")
        if policy == "continue":
            j["term_sent_at"] = iso(utcnow()); j["policy_continue"] = True
            self.signal("JOB_OVERRUN", "yellow", f"#{bid} job pid {j['pid']} continues past its slot by request (on_slot_end=continue): {why}", booking_id=bid)
            return
        try:
            os.killpg(j["pid"], signal.SIGTERM)          # the agent's OWN child only, once, graceful; never SIGKILL
        except ProcessLookupError:
            pass
        except PermissionError as e:
            log.warning("SIGTERM to %s refused: %s", j["pid"], e)
        j["term_sent_at"] = iso(utcnow())
        self.signal("JOB_SLOT_END_TERM", "yellow", f"#{bid} job pid {j['pid']} sent SIGTERM (its own process group) at slot end: {why}. "
                    f"Nothing else is touched.", booking_id=bid)

    def finish_job(self, bid: int, j: dict, rc: Optional[int], b: Optional[dict], online: bool) -> None:
        logp = Path(j["log"]); copy_path = None
        try:
            if self.a.log_dir:
                d = Path(self.a.log_dir).expanduser() / self.rid; d.mkdir(parents=True, exist_ok=True)
                copy_path = d / f"{bid}_{utcnow():%Y%m%d}.log"
                shutil.copyfile(logp, copy_path)
        except OSError as e:
            log.warning("log copy to --log-dir failed: %s", e); copy_path = None
        note = "" if rc is not None else "agent restarted during the job; exit code unknown"
        if j.get("term_sent_at") and not j.get("policy_continue"):
            note = (note + " | " if note else "") + "terminated at slot end (SIGTERM)"
        self.send("POST", f"/bookings/{bid}/job", {"event": "exited", "pid": j["pid"], "exit_code": rc,
                                                   "log_path": str(copy_path or logp), "from_cache": j.get("from_cache", False),
                                                   "cmd": j.get("cmd"), "cwd": j.get("cwd"), "started_at": j.get("started_at"),
                                                   "ended_at": iso(utcnow()), "note": note})
        if j.get("lock_path"):
            lock_release(j["lock_path"], f"gpub:{bid}")
        release_on_exit = ((j.get("snapshot") or {}).get("job") or {}).get("release_on_exit", True)
        if online and release_on_exit and b and b.get("state") == "active" and not j.get("term_sent_at"):
            st, js = self.send("POST", f"/bookings/{bid}/release", queue_if_offline=False)
            log.info("released #%s after job exit (%s)", bid, st)
        log.info("job #%s finished rc=%s", bid, rc)
        self.procs.pop(bid, None)
        self.st["jobs"].pop(str(bid), None)
        self.st["done"] = (self.st["done"] + [bid])[-200:]

    # ---- one pass
    def act(self, plan: Optional[dict], online: bool, gpu: dict, oll: dict) -> None:
        now = utcnow()
        bookings = (plan or {}).get("bookings") or []
        resource = (plan or {}).get("resource") or {}
        by_id = {b["id"]: b for b in bookings}
        live_sids = set()
        for bid in list(self.procs):
            if self.procs[bid].poll() is None:
                live_sids.add(self.procs[bid].pid)
        for k, j in self.st["jobs"].items():
            live_sids.add(j["pid"])

        for b in bookings:
            bid, start, end = b["id"], parse(b["start"]), parse(b["end"])
            in_slot = start <= now < end
            exclusive = b["mode"] == "exclusive"
            # Ollama yield before an exclusive slot (the only "enforcement" action in v1; it is a request to Ollama, not a kill)
            if exclusive and b["state"] in ("booked", "active") and (start - now).total_seconds() <= self.a.unload_lead and now < end:
                loaded = [m["name"] for m in oll.get("loaded", []) if m.get("name")]
                if loaded and not self.a.dry:
                    done = [m for m in loaded if ollama_unload(self.a.ollama, m, self.a.timeout)]
                    if done:
                        self.signal("OLLAMA_UNLOADED", "green", f"{self.rid}: unloaded {', '.join(done)} ahead of exclusive #{bid} "
                                    f"({b['owner_machine']}: {b.get('purpose','')[:50]})", booking_id=bid, to=[b["owner_machine"], "*"])
                        oll = ollama_ps(self.a.ollama, self.a.timeout)
            # start the job
            if b.get("job") and str(bid) not in self.st["jobs"] and bid not in self.st["done"]:
                if online and b["state"] == "active":
                    self.start_job(b, from_cache=False, resource=resource)
                elif not online and b["state"] in ("booked", "active") and in_slot and not self.skew_bad:
                    self.start_job(b, from_cache=True, resource=resource)
            # squatter watch during an exclusive active slot (observe + flag; never act)
            if exclusive and b["state"] == "active" and in_slot:
                for app in gpu.get("apps") or []:
                    pid = app.get("pid")
                    if not pid or pid == os.getpid() or "ollama" in (app.get("name") or "").lower():
                        continue
                    try:
                        sid = os.getsid(pid)
                    except (ProcessLookupError, PermissionError):
                        sid = -1
                    if sid in live_sids:
                        continue
                    key = f"{bid}:{pid}"
                    if key in self.st["squatters"]:
                        continue
                    self.st["squatters"] = (self.st["squatters"] + [key])[-500:]
                    self.signal("SQUATTER_DETECTED", "yellow",
                                f"{self.rid}: pid {pid} {app.get('name')} ({app.get('mem_mb')} MB) is on the GPU during exclusive #{bid} "
                                f"owned by {b['owner_machine']} and is not the booked job. Soft enforcement: not killed.",
                                booking_id=bid, to=[b["owner_machine"], "*"])

        # manage running jobs (also those whose booking left the plan: released/expired/cancelled)
        for k in list(self.st["jobs"]):
            bid = int(k); j = self.st["jobs"][k]
            b = by_id.get(bid) or (j.get("snapshot") or {})
            alive, rc = self._alive(bid, j)
            if not alive:
                self.finish_job(bid, j, rc, by_id.get(bid), online); continue
            end = parse(b["end"]) if b.get("end") else None
            if online:
                st, hb = self.send("POST", f"/bookings/{bid}/heartbeat", queue_if_offline=False)
                if st == 200 and hb.get("state") != "active":
                    self._term(bid, j, f"booking is {hb.get('state')} ({hb.get('end_reason')})")
                    continue
                if st == 200:
                    end = parse(hb["ends"])
                if end and (end - now).total_seconds() <= self.a.extend_lead and j.get("extend_refused_for") != iso(end) \
                        and ((j.get("snapshot") or {}).get("job") or {}).get("extend", True) and not j.get("term_sent_at"):
                    st2, ex = self.send("POST", f"/bookings/{bid}/extend", {"minutes": self.a.extend_min, "reason": "job still running"},
                                        queue_if_offline=False)
                    if st2 == 200:
                        log.info("#%s extended to %s", bid, ex.get("end")); end = parse(ex["end"])
                    elif st2 is not None:
                        j["extend_refused_for"] = iso(end); log.info("#%s extension refused: %s", bid, str(ex)[:120])
            else:
                # offline: self-extend from the cache only if nothing in the cache starts before now+extend_min
                if end and now >= end and not j.get("term_sent_at"):
                    horizon = now + dt.timedelta(minutes=self.a.extend_min)
                    nxt = [x for x in bookings if x["id"] != bid and parse(x["start"]) < horizon and parse(x["end"]) > now]
                    if not nxt and not self.skew_bad:
                        j["cache_extended_until"] = iso(horizon); log.info("#%s self-extended from cache to %s", bid, horizon)
                        b["end"] = iso(horizon); j["snapshot"]["end"] = iso(horizon); end = horizon
            if end and now >= end and not j.get("term_sent_at"):
                self._term(bid, j, "slot end reached" + ("" if online else " (coordinator unreachable)"))
            if j.get("term_sent_at") and not j.get("overrun_flagged") and not j.get("policy_continue") \
                    and (now - parse(j["term_sent_at"])).total_seconds() > 60:
                j["overrun_flagged"] = True
                self.signal("JOB_OVERRUN", "yellow", f"#{bid} job pid {j['pid']} still running 60 s after SIGTERM; left alone (soft).", booking_id=bid)

    def reconcile(self) -> None:
        for k, j in list(self.st["jobs"].items()):
            alive, _ = self._alive(int(k), j)
            log.info("reconcile: job #%s pid %s %s", k, j["pid"], "still running (adopted)" if alive else "gone")
        n = len(self.st["jobs"]); c = self.load_cache()
        self.signal("AGENT_NOTE", "green", f"{VERSION} started on {self.machine} for {self.rid} (pid {os.getpid()}, poll {self.a.poll:g} s, "
                    f"nvidia-smi {'ok' if self.smi else 'MISSING'}, cache {len((c or {}).get('bookings') or [])} booking(s), "
                    f"{n} job(s) adopted)")

    def loop(self) -> None:
        self.check_skew()
        self.reconcile()
        while True:
            t0 = time.time()
            try:
                gpu = sample_gpu(self.smi); oll = ollama_ps(self.a.ollama, self.a.timeout)
                plan, online = self.fetch_plan()
                self.on_transition(online)
                if online:
                    running = [k for k in self.st["jobs"]]
                    self.send("POST", f"/agents/{self.rid}/report", {
                        "gpu": gpu, "ollama": oll,
                        "agent": {"version": VERSION, "pid": os.getpid(), "uptime_s": round((utcnow() - self.started).total_seconds()),
                                  "skew_s": round(self.skew_s, 3), "jobs_running": len(running), "pending": len(self.st["pending"]),
                                  "host": socket.gethostname(), "nvidia_smi": self.smi}}, queue_if_offline=False)
                self.act(plan, online, gpu, oll)
                self.save()
            except Exception:
                log.exception("agent pass failed")
            if self.a.once:
                return
            time.sleep(max(0.5, self.a.poll - (time.time() - t0)))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--resource", required=True)
    ap.add_argument("--poll", type=float, default=30.0)
    ap.add_argument("--state-dir", default=str(Path.home() / ".local" / "state" / "gpub"))
    ap.add_argument("--nvidia-smi", default=None)
    ap.add_argument("--ollama", default="http://127.0.0.1:11434")
    ap.add_argument("--log-dir", default=str(Path.home() / ".local" / "state" / "gpub" / "jobs"),
                    help="copy each job log to <log-dir>/<resource>/<id>_<date>.log on exit ('' = no copy)")
    ap.add_argument("--timeout", type=float, default=8.0)
    ap.add_argument("--unload-lead", type=float, default=90.0, help="seconds before an exclusive slot to ask Ollama to unload")
    ap.add_argument("--extend-lead", type=float, default=60.0, help="seconds before slot end to request an extension for a running job")
    ap.add_argument("--extend-min", type=int, default=15)
    ap.add_argument("--once", action="store_true"); ap.add_argument("--dry", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ag = Agent(a)
    def _stop(sig, _f):
        ag.save(); log.info("agent stopping on signal %s; jobs keep running and are reconciled on restart", sig); sys.exit(0)
    signal.signal(signal.SIGTERM, _stop); signal.signal(signal.SIGINT, _stop)
    ag.loop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
