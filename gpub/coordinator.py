#!/usr/bin/env python3
"""coordinator.py — GPUB (GPU Booker) coordinator: one small FastAPI service in front of a Postgres ledger.

  coordinator.py initdb   [--db gpub] [--hosts hosts.json] [--lock-path PATH]   create db + schema + migrations + seed
  coordinator.py register <machine> --ip <machine's private IP> [--os linux] [--db gpub]   issue a token (printed ONCE)
  coordinator.py serve    [--db gpub] [--bind 127.0.0.1] [--port 8766] [--tick 10]

Host agent contract:
  GET  /agents/{resource}/plan      (host agent) 7-day plan incl. full job payloads + resource row -> the agent's local cache
  POST /agents/{resource}/report    (host agent) telemetry sample (nvidia-smi fields that exist; N/A -> null) + Ollama state
  POST /bookings/{id}/job           (host agent) job started/exited -> jobs table + JOB_* signals
  POST /bookings/{id}/extend        (owner or host agent) +N min while a job is still running; the DB decides
  POST /signals                     (any machine) emit an allow-listed signal (SQUATTER_DETECTED, OLLAMA_UNLOADED, ...)
  heartbeat/release accept the resource's HOST machine as well as the owner (the agent acts for the slot it runs).
  Public GET /bookings redacts job.env VALUES (keys only); the full job goes to the host agent alone.
  Coordinator flags AGENT_DOWN / AGENT_UP from telemetry age.  Still soft: this process never kills anything.

Group ("pair") resources: resources.members marks a group; booking it inserts one exclusive member booking per member in
the same transaction (the job rides on the DRIVER member = the member hosted by the group's host_machine, so host agents
are unchanged); release/expiry/slot-end/cancel/extend act on the whole group; a taken member slot refuses the group outright.

Purpose: a few GPUs, pre-scheduled, so bots/tools/LLM agents/renderers run back to back instead of colliding.
Design defaults:
  1. Optional external lockfile wrap: when a booking on a resource with gpu_lock_path goes active the coordinator takes
     that lockfile on the holder's behalf (JSON body, O_EXCL, coordinator pid) and releases it with the lease. A lock
     held by a legacy tool delays the slot (SLOT_DELAYED) — it never bumps it.
  2. Signals live here (stream + acks).
  3. Enforcement is soft/advisory only.  This process never kills anything.

The DATABASE prevents conflicts (EXCLUDE constraint + capacity trigger).  This API just translates errors.
Identity: Bearer token -> machine row, AND request source IP must equal that machine's registered IP (tailnet_ip column).
Nothing is ever deleted; DELETE is trigger-blocked in the schema.
"""

import argparse, asyncio, datetime as dt, hashlib, json, logging, os, secrets, socket, sys, time
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import psycopg
from psycopg import errors as pgerr
from psycopg.rows import dict_row
from psycopg.types.range import Range
from fastapi import FastAPI, HTTPException, Request, Depends, Query
from fastapi.responses import PlainTextResponse, HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

HERE = Path(__file__).resolve().parent
SQL_DIR = HERE.parent / "sql"
SCHEMA = SQL_DIR / "schema.sql"
MIGRATIONS = [SQL_DIR / "migration_01_agents.sql", SQL_DIR / "migration_02_pairs.sql"]   # applied after SCHEMA, in order
LOCK_MAX_HOLD_H = 6.0            # stale-lock age; must match the max-hold rule of any legacy tool sharing the lockfile
PRIORITY_RANK = {"critical": 0, "sweep": 1, "interactive": 2, "background": 3}
VERSION = "gpub-coordinator 1.0"
AGENT_STALE_S = float(os.environ.get("GPUB_AGENT_STALE_S", "300"))   # telemetry older than this -> AGENT_DOWN (yellow); env only for the gate fixture
AGENT_SIGNAL_TYPES = {"SQUATTER_DETECTED", "OLLAMA_UNLOADED", "COORDINATOR_DEGRADED", "JOB_OVERRUN", "JOB_SLOT_END_TERM",
                      "CLOCK_SKEW", "AGENT_UP", "AGENT_NOTE", "LANE_BLOCKED"}
EXTEND_MAX_MIN = 240             # one extension request may not exceed this

log = logging.getLogger("gpub")


# ------------------------------------------------------------------ db helpers
def connect(db: str) -> psycopg.Connection:
    return psycopg.connect(f"dbname={db}", row_factory=dict_row, autocommit=False)


def ensure_db(db: str) -> None:
    with psycopg.connect("dbname=postgres", autocommit=True) as c:
        exists = c.execute("SELECT 1 FROM pg_database WHERE datname=%s", (db,)).fetchone()
        if not exists:
            c.execute(f'CREATE DATABASE "{db}"')
            print(f"[initdb] created database {db}")
        else:
            print(f"[initdb] database {db} exists")


def apply_schema(db: str) -> None:
    with connect(db) as c:
        c.execute(SCHEMA.read_text())
        for m in MIGRATIONS:
            c.execute(m.read_text())
        c.commit()
    print(f"[initdb] schema applied from {SCHEMA.name} + {', '.join(m.name for m in MIGRATIONS)}")


def seed(db: str, hosts_path: Optional[Path], lock_path: Optional[str]) -> None:
    """Seed the example resources + machines, optionally enriched from a hosts file (see examples/hosts.example.json).
    Idempotent, additive only. Edit these lists (or add rows with SQL, see examples/) to describe your own hardware."""
    hosts = json.loads(hosts_path.read_text())["hosts"] if hosts_path and hosts_path.exists() else []
    by_key = {h["key"]: h for h in hosts}
    machines = [  # (machine name, key in the hosts file)
        ("headnode", "headnode"), ("workstation", "workstation"), ("rtxbox", "rtxbox"), ("spark-a", "spark-a"),
    ]
    resources = [
        ("rtxbox-5090", "rtxbox", "NVIDIA GeForce RTX 5090", 32, lock_path,
         "32 GB VRAM. If the host is WSL2 (not native Linux), per-process GPU attribution is not available from WSL."),
        ("spark-a-gb10", "spark-a", "NVIDIA GB10 (DGX Spark)", 110, None,
         "Unified ~121 GB; nvidia-smi reports memory N/A. 110 GB budget leaves OS headroom. Configured, not measured."),
    ]
    with connect(db) as c:
        for m, key in machines:
            h = by_key.get(key, {})
            c.execute(
                """INSERT INTO machines (machine, tailnet_name, tailnet_ip, os)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (machine) DO UPDATE SET tailnet_name = EXCLUDED.tailnet_name,
                        tailnet_ip = COALESCE(EXCLUDED.tailnet_ip, machines.tailnet_ip)""",
                (m, h.get("tailnet_name"), h.get("tailnet_ip"), (h.get("runtime") or h.get("role") or "")[:60]),
            )
        for rid, host, model, cap, lock, notes in resources:
            c.execute(
                """INSERT INTO resources (resource_id, host_machine, model, capacity_gb, gpu_lock_path, notes)
                   VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (resource_id) DO NOTHING""",
                (rid, host, model, cap, lock, notes),
            )
        c.commit()
    print(f"[initdb] seeded {len(machines)} machines, {len(resources)} resources")


def register(db: str, machine: str, ip: Optional[str], os_name: Optional[str]) -> str:
    token = "gpub_" + secrets.token_urlsafe(32)
    h = hashlib.sha256(token.encode()).hexdigest()
    with connect(db) as c:
        c.execute(
            """INSERT INTO machines (machine, tailnet_ip, os, token_hash) VALUES (%s, %s, %s, %s)
               ON CONFLICT (machine) DO UPDATE SET token_hash = EXCLUDED.token_hash,
                    tailnet_ip = COALESCE(EXCLUDED.tailnet_ip, machines.tailnet_ip),
                    os = COALESCE(EXCLUDED.os, machines.os)""",
            (machine, ip, os_name, h),
        )
        c.commit()
    return token


# ------------------------------------------------------------------ optional external lockfile wrap
def _lock_read(path: str) -> Optional[dict]:
    try:
        return json.loads(Path(path).read_text())
    except Exception:
        return None


def _lock_age_h(info: dict) -> float:
    try:
        t = dt.datetime.fromisoformat(info["started"])
        return (dt.datetime.now() - t).total_seconds() / 3600.0
    except Exception:
        return 1e9


def lock_holder_name(booking_id: int) -> str:
    return f"gpub:{booking_id}"


def lock_acquire(path: str, holder: str, note: str) -> tuple[bool, Optional[dict]]:
    """Non-blocking lockfile acquire (JSON body, O_EXCL) with the coordinator's own pid as the recorded pid.
    Returns (acquired, blocking_info). Reclaims only STALE (>6h) locks, never a live foreign holder."""
    info = _lock_read(path)
    if info and info.get("holder") == holder:
        return True, None
    if info and _lock_age_h(info) > LOCK_MAX_HOLD_H:
        log.warning("gpu_lock: reclaiming STALE lock from %r (%.1fh)", info.get("holder"), _lock_age_h(info))
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        info = None
    if info:
        return False, info
    payload = json.dumps({
        "holder": holder, "host": socket.gethostname(), "pid": os.getpid(),
        "started": dt.datetime.now().isoformat(timespec="seconds"), "note": note,
    })
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        os.write(fd, payload.encode())
        os.close(fd)
        return True, None
    except FileExistsError:
        return False, _lock_read(path)


def lock_release(path: str, holder: str) -> bool:
    """Stricter than the legacy release: only remove a lock WE hold."""
    info = _lock_read(path)
    if not info:
        return True
    if info.get("holder") != holder:
        log.warning("gpu_lock: NOT releasing lock held by %r (we are %r)", info.get("holder"), holder)
        return False
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    return True


# ------------------------------------------------------------------ signals
def emit(c: psycopg.Connection, *, type_: str, severity: str, message: str, from_machine: str = "coordinator",
         to: Optional[list[str]] = None, resource_id: Optional[str] = None, booking_id: Optional[int] = None,
         ack_required: bool = False) -> str:
    n = c.execute("SELECT nextval('signal_seq') AS n").fetchone()["n"]
    sid = f"DS-{dt.datetime.now(dt.UTC).year}-{n:06d}"
    c.execute(
        """INSERT INTO signals (signal_id, from_machine, to_machines, type, severity, resource_id, booking_id,
                                message, ack_required) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (sid, from_machine, to or ["*"], type_, severity, resource_id, booking_id, message, ack_required),
    )
    log.info("signal %s %s %s: %s", sid, severity.upper(), type_, message)
    return sid


def signal_exists(c, type_: str, booking_id: int) -> bool:
    return c.execute("SELECT 1 FROM signals WHERE type=%s AND booking_id=%s LIMIT 1",
                     (type_, booking_id)).fetchone() is not None


# ------------------------------------------------------------------ scheduler loop
def rrule_expand(c: psycopg.Connection, horizon_days: int = 14) -> None:
    """Materialise occurrences of 'template' bookings into child 'booked' rows, in the owner's tz."""
    from dateutil.rrule import rrulestr
    now = dt.datetime.now(dt.UTC)
    horizon = now + dt.timedelta(days=horizon_days)
    for t in c.execute("SELECT * FROM bookings WHERE state='template'").fetchall():
        tres = c.execute("SELECT * FROM resources WHERE resource_id=%s", (t["resource_id"],)).fetchone() or {}
        try:
            tz = ZoneInfo(t["tz"])
        except Exception:
            tz = dt.UTC
        slot: Range = t["slot"]
        start_local = slot.lower.astimezone(tz)
        dur = slot.upper - slot.lower
        try:
            rule = rrulestr(t["rrule"], dtstart=start_local.replace(tzinfo=None))
        except Exception as e:
            log.error("template %s has bad rrule %r: %s", t["id"], t["rrule"], e)
            continue
        for occ in rule.between(now.astimezone(tz).replace(tzinfo=None) - dt.timedelta(minutes=1),
                                horizon.astimezone(tz).replace(tzinfo=None), inc=True):
            occ_utc = occ.replace(tzinfo=tz).astimezone(dt.UTC)    # wall-clock in tz -> UTC: DST-correct
            if c.execute("SELECT 1 FROM bookings WHERE parent_id=%s AND lower(slot)=%s",
                         (t["id"], occ_utc)).fetchone():
                continue
            try:
                with c.transaction():
                    c.execute("SELECT set_config('gpub.actor', 'coordinator', true)")
                    occ = c.execute(
                        """INSERT INTO bookings (resource_id, owner_machine, mode, vram_budget_gb, priority, state,
                             purpose, slot, tz, parent_id, job, enforce, grace_min)
                           VALUES (%s,%s,%s,%s,%s,'booked',%s,tstzrange(%s,%s,'[)'),%s,%s,%s,%s,%s) RETURNING *""",
                        (t["resource_id"], t["owner_machine"], t["mode"], t["vram_budget_gb"], t["priority"],
                         t["purpose"], occ_utc, occ_utc + dur, t["tz"], t["id"], json.dumps(t["job"]) if t["job"] else None,
                         t["enforce"], t["grace_min"]),
                    ).fetchone()
                    if tres.get("members"):
                        pair_members_insert(c, occ, tres)
            except (pgerr.ExclusionViolation, pgerr.RaiseException, pgerr.ForeignKeyViolation) as e:
                emit(c, type_="BOOKING_CONFLICT", severity="yellow", resource_id=t["resource_id"], booking_id=t["id"],
                     to=[t["owner_machine"]],
                     message=f"recurrence of template #{t['id']} at {occ_utc:%Y-%m-%d %H:%MZ} could not be placed: "
                             f"{str(e).splitlines()[0]}")


def tick(db: str) -> None:
    now = dt.datetime.now(dt.UTC)
    with connect(db) as c:
        c.execute("SELECT set_config('gpub.actor', 'coordinator', false)")
        rrule_expand(c)

        # 1. booked -> active at slot start (take the external lockfile first where the resource has one)
        due = c.execute(
            """SELECT b.*, r.gpu_lock_path FROM bookings b JOIN resources r USING (resource_id)
                WHERE b.state='booked' AND lower(b.slot) <= %s AND upper(b.slot) > %s
                ORDER BY lower(b.slot)""", (now, now)).fetchall()
        for b in due:
            held = False
            if b["gpu_lock_path"]:
                ok, blocker = lock_acquire(b["gpu_lock_path"], lock_holder_name(b["id"]),
                                           f"gpub booking {b['id']} {b['owner_machine']}: {b['purpose'][:60]}")
                if not ok:
                    if not signal_exists(c, "SLOT_DELAYED", b["id"]):
                        emit(c, type_="SLOT_DELAYED", severity="yellow", resource_id=b["resource_id"],
                             booking_id=b["id"], to=[b["owner_machine"]],
                             message=f"booking #{b['id']} cannot start: external lockfile held by "
                                     f"'{(blocker or {}).get('holder')}' on {(blocker or {}).get('host')} "
                                     f"since {(blocker or {}).get('started')}. Will retry each tick.")
                    continue
                held = True
            c.execute("UPDATE bookings SET state='active' WHERE id=%s", (b["id"],))
            c.execute("INSERT INTO leases (booking_id, holder_machine, gpu_lock_held) VALUES (%s,%s,%s)",
                      (b["id"], b["owner_machine"], held))
            emit(c, type_="SLOT_STARTED", severity="green", resource_id=b["resource_id"], booking_id=b["id"],
                 to=[b["owner_machine"], "*"],
                 message=f"#{b['id']} {b['mode']} {b['resource_id']} for {b['owner_machine']} started "
                         f"({b['purpose'][:60]}); ends {b['slot'].upper:%H:%MZ}"
                         + (f"; heartbeat required within {b['grace_min']} min" if b["grace_min"] else ""))
        c.commit()

        # 2. heartbeat loss / no-show -> expired
        stale = c.execute(
            """SELECT b.*, l.last_heartbeat, l.heartbeats, r.gpu_lock_path
                 FROM bookings b JOIN leases l ON l.booking_id=b.id JOIN resources r USING (resource_id)
                WHERE b.state='active' AND b.grace_min > 0
                  AND l.last_heartbeat < %s - make_interval(mins => b.grace_min)""", (now,)).fetchall()
        for b in stale:
            reason = "no_show" if b["heartbeats"] == 0 else "heartbeat_lost"
            _end(c, b, "expired", reason)
            emit(c, type_="LEASE_EXPIRED", severity="yellow", resource_id=b["resource_id"], booking_id=b["id"],
                 to=[b["owner_machine"], "*"],
                 message=f"#{b['id']} on {b['resource_id']} expired ({reason}): last heartbeat "
                         f"{b['last_heartbeat']:%H:%M:%SZ}, grace {b['grace_min']} min. Slot released.")
        c.commit()

        # 3. slot end -> released
        over = c.execute(
            """SELECT b.*, r.gpu_lock_path FROM bookings b JOIN resources r USING (resource_id)
                WHERE b.state='active' AND upper(b.slot) <= %s""", (now,)).fetchall()
        for b in over:
            _end(c, b, "released", "slot_end")
            emit(c, type_="SLOT_RELEASED", severity="green", resource_id=b["resource_id"], booking_id=b["id"],
                 to=[b["owner_machine"], "*"], message=f"#{b['id']} on {b['resource_id']} reached slot end; released.")
        # 4. booked slots that are already entirely in the past (coordinator was down) -> expired, flagged
        missed = c.execute("SELECT * FROM bookings WHERE state='booked' AND upper(slot) <= %s", (now,)).fetchall()
        for b in missed:
            c.execute("UPDATE bookings SET state='expired', end_reason='missed_while_coordinator_down', ended_at=now() "
                      "WHERE id=%s", (b["id"],))
            emit(c, type_="SLOT_MISSED", severity="yellow", resource_id=b["resource_id"], booking_id=b["id"],
                 to=[b["owner_machine"]], message=f"#{b['id']} was never activated (coordinator down during slot).")
        c.commit()
        # 5. agents: stale telemetry -> AGENT_DOWN / AGENT_UP
        agent_watch(c)
        c.commit()


_agent_state: dict[str, str] = {}      # resource_id -> 'up' | 'down' (process-local; re-derived on restart)


def agent_watch(c) -> None:
    """AGENT_DOWN once when a resource's telemetry goes stale; AGENT_UP when it returns. Never for resources that
    have never reported (no agent installed yet is not an error)."""
    rows = c.execute("""SELECT r.resource_id, r.host_machine, max(t.ts) AS last FROM resources r
                        JOIN telemetry t USING (resource_id) GROUP BY 1,2""").fetchall()
    now = dt.datetime.now(dt.UTC)
    for r in rows:
        age = (now - r["last"]).total_seconds()
        st = "down" if age > AGENT_STALE_S else "up"
        prev = _agent_state.get(r["resource_id"])
        if prev is None:
            _agent_state[r["resource_id"]] = st
            if st == "down":
                emit(c, type_="AGENT_DOWN", severity="yellow", resource_id=r["resource_id"], to=[r["host_machine"], "*"],
                     message=f"{r['resource_id']} agent on {r['host_machine']} silent for {age:.0f} s "
                             f"(last report {r['last']:%m-%d %H:%M}Z). Jobs on this GPU will not start.")
        elif st != prev:
            _agent_state[r["resource_id"]] = st
            if st == "down":
                emit(c, type_="AGENT_DOWN", severity="yellow", resource_id=r["resource_id"], to=[r["host_machine"], "*"],
                     message=f"{r['resource_id']} agent on {r['host_machine']} silent for {age:.0f} s "
                             f"(last report {r['last']:%m-%d %H:%M}Z). Jobs on this GPU will not start.")
            else:
                emit(c, type_="AGENT_UP", severity="green", resource_id=r["resource_id"], to=["*"],
                     message=f"{r['resource_id']} agent on {r['host_machine']} reporting again.")


# ------------------------------------------------------------------ group ("pair") resources
def pair_group(c, b: dict) -> list[dict]:
    """Every OTHER live row in b's pair group: for a member, its pair row + sibling members; for a pair row, its
    members. Empty for an ordinary booking. Live = booked|active."""
    pid = b.get("pair_id")
    if pid:
        return c.execute("""SELECT b.*, r.gpu_lock_path FROM bookings b JOIN resources r USING (resource_id)
                             WHERE (b.id=%s OR b.pair_id=%s) AND b.id<>%s AND b.state IN ('booked','active')""",
                         (pid, pid, b["id"])).fetchall()
    return c.execute("""SELECT b.*, r.gpu_lock_path FROM bookings b JOIN resources r USING (resource_id)
                         WHERE b.pair_id=%s AND b.state IN ('booked','active')""", (b["id"],)).fetchall()


def pair_members_insert(c, pr: dict, r: dict) -> list[dict]:
    """Insert one EXCLUSIVE member booking per member of pair resource r for pair row pr, same slot/owner/purpose.
    The job (if any) rides on the DRIVER member (the member hosted by the pair's host_machine) so the existing host
    agent runs it unchanged; that member keeps the heartbeat grace, every other row in the group gets grace 0
    (nobody heartbeats a hold). Without a job the pair row keeps the grace (an interactive `gpub run` heartbeats it).
    Any member slot already taken raises from the members' own EXCLUDE/capacity checks -> the caller's transaction
    rolls back -> no partial hold."""
    hosts = {x["resource_id"]: x["host_machine"] for x in
             c.execute("SELECT resource_id, host_machine FROM resources WHERE resource_id = ANY(%s)", (r["members"],)).fetchall()}
    missing = [m for m in r["members"] if m not in hosts]
    if missing:
        raise pgerr.ForeignKeyViolation(f"gpub: pair {r['resource_id']} names unknown member(s) {missing}")
    driver = next((m for m in r["members"] if hosts[m] == r["host_machine"]), r["members"][0])
    job = pr.get("job")
    out = []
    for mid in r["members"]:
        carries_job = bool(job) and mid == driver
        row = c.execute(
            """INSERT INTO bookings (resource_id, owner_machine, mode, priority, state, purpose, slot, tz, pair_id, job,
                 enforce, grace_min)
               VALUES (%s,%s,'exclusive',%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
            (mid, pr["owner_machine"], pr["priority"], pr["state"], f"[{r['resource_id']} #{pr['id']}] {pr['purpose']}",
             pr["slot"], pr["tz"], pr["id"], json.dumps(job) if carries_job else None, pr["enforce"],
             pr["grace_min"] if carries_job else 0)).fetchone()
        out.append(row)
    if job:   # the driver member's agent heartbeats; the pair row must not expire for want of one
        c.execute("UPDATE bookings SET grace_min=0 WHERE id=%s", (pr["id"],))
    return out


def _end(c, b: dict, state: str, reason: str) -> None:
    _end_one(c, b, state, reason)
    for sib in pair_group(c, b):            # a pair ends as one (member release/expiry/slot end -> whole group)
        _end_one(c, sib, state, reason if reason.endswith("_pair") else reason + "_pair")


def _end_one(c, b: dict, state: str, reason: str) -> None:
    c.execute("UPDATE bookings SET state=%s, end_reason=%s, ended_at=now() WHERE id=%s", (state, reason, b["id"]))
    c.execute("UPDATE leases SET state=%s, ended_at=now() WHERE booking_id=%s AND state='active'",
              ("released" if state == "released" else "expired", b["id"]))
    if b.get("gpu_lock_path"):
        lock_release(b["gpu_lock_path"], lock_holder_name(b["id"]))


# ------------------------------------------------------------------ API
def build_app(db: str):
    app = FastAPI(title="GPU Booker coordinator", version=VERSION)

    def conn():
        return connect(db)

    async def auth(request: Request) -> dict:
        hdr = request.headers.get("authorization", "")
        if not hdr.lower().startswith("bearer "):
            raise HTTPException(401, "missing bearer token")
        h = hashlib.sha256(hdr[7:].strip().encode()).hexdigest()
        with conn() as c:
            m = c.execute("SELECT * FROM machines WHERE token_hash=%s AND enabled", (h,)).fetchone()
            if not m:
                raise HTTPException(401, "unknown token")
            src = request.client.host if request.client else None
            if m["tailnet_ip"] is None or str(m["tailnet_ip"]) != src:
                log.warning("identity mismatch: token of %r presented from %s", m["machine"], src)
                raise HTTPException(403, f"token belongs to '{m['machine']}' but request came from {src}")
            c.execute("UPDATE machines SET last_seen=now() WHERE machine=%s", (m["machine"],))
            c.commit()
        return m

    def to_json(row: dict) -> dict:
        out = {}
        for k, v in row.items():
            if isinstance(v, Range):
                out["start"] = v.lower.isoformat(); out["end"] = v.upper.isoformat()
            elif isinstance(v, (dt.datetime,)):
                out[k] = v.isoformat()
            elif hasattr(v, "__float__") and not isinstance(v, (int, float, bool)):
                out[k] = float(v)
            else:
                out[k] = v
        out.pop("slot", None)
        return out

    class BookingIn(BaseModel):
        resource_id: str
        mode: str = "exclusive"
        vram_budget_gb: Optional[float] = None
        priority: str = "interactive"
        start: Optional[dt.datetime] = None           # None = now (used by /bookings/earliest)
        duration_min: int = Field(gt=0, le=7 * 24 * 60)
        purpose: str = ""
        rrule: Optional[str] = None
        tz: str = "UTC"
        enforce: str = "soft"
        grace_min: int = 5
        job: Optional[dict] = None

    def insert_booking(c, m: dict, p: BookingIn, start: dt.datetime, state: str = "booked") -> dict:
        end = start + dt.timedelta(minutes=p.duration_min)
        c.execute("SELECT set_config('gpub.actor', %s, true)", (m["machine"],))
        row = c.execute(
            """INSERT INTO bookings (resource_id, owner_machine, mode, vram_budget_gb, priority, state, purpose,
                 slot, tz, rrule, job, enforce, grace_min)
               VALUES (%s,%s,%s,%s,%s,%s,%s,tstzrange(%s,%s,'[)'),%s,%s,%s,%s,%s) RETURNING *""",
            (p.resource_id, m["machine"], p.mode, p.vram_budget_gb, p.priority, state, p.purpose, start, end,
             p.tz, p.rrule, json.dumps(p.job) if p.job else None, p.enforce, p.grace_min)).fetchone()
        r = c.execute("SELECT * FROM resources WHERE resource_id=%s", (p.resource_id,)).fetchone()
        if r and r.get("members"):                      # a pair is booked exclusive, as one, or not at all
            if p.mode != "exclusive":
                raise HTTPException(422, f"{p.resource_id} is a pair resource: exclusive bookings only")
            if state != "template":
                pair_members_insert(c, row, r)
        return row

    def redact(b: dict) -> dict:
        """Public views never carry job.env VALUES (no secrets on the unauthenticated surface); keys stay visible."""
        j = b.get("job")
        if isinstance(j, dict) and isinstance(j.get("env"), dict):
            b = b | {"job": j | {"env": {k: "<redacted>" for k in j["env"]}}}
        return b

    def conflict(e: Exception) -> HTTPException:
        msg = str(e).splitlines()[0]
        if isinstance(e, pgerr.ExclusionViolation) and "no_exclusive_overlap" in msg:
            msg = "exclusive slot overlaps a live exclusive booking (Postgres EXCLUDE constraint)"
        return HTTPException(409, msg)

    @app.get("/time")
    def time_():
        return {"utc": dt.datetime.now(dt.UTC).isoformat(), "version": VERSION}

    @app.get("/resources")
    def resources():
        with conn() as c:
            return [to_json(r) for r in c.execute("SELECT * FROM resources ORDER BY resource_id").fetchall()]

    def _status(c) -> list[dict]:
        now = dt.datetime.now(dt.UTC)
        out = []
        for r in c.execute("SELECT * FROM resources ORDER BY resource_id").fetchall():
            active = c.execute(
                """SELECT b.id, b.owner_machine, b.mode, b.vram_budget_gb, b.priority, b.purpose, b.slot,
                          l.last_heartbeat, l.heartbeats
                     FROM bookings b LEFT JOIN leases l ON l.booking_id=b.id
                    WHERE b.resource_id=%s AND b.state='active' ORDER BY lower(b.slot)""", (r["resource_id"],)).fetchall()
            nxt = c.execute(
                """SELECT id, owner_machine, mode, vram_budget_gb, priority, purpose, slot FROM bookings
                    WHERE resource_id=%s AND state='booked' AND lower(slot) > %s ORDER BY lower(slot) LIMIT 3""",
                (r["resource_id"], now)).fetchall()
            lock = _lock_read(r["gpu_lock_path"]) if r["gpu_lock_path"] else None
            t = c.execute("SELECT * FROM telemetry WHERE resource_id=%s ORDER BY ts DESC LIMIT 1", (r["resource_id"],)).fetchone()
            gpu = agent = None
            if t:
                age = (now - t["ts"]).total_seconds()
                gpu = (t["gpu"] or {}) | {"ts": t["ts"].isoformat(), "age_s": round(age), "stale": age > AGENT_STALE_S,
                                          "ollama": t["ollama"]}
                agent = (t["agent"] or {}) | {"machine": t["machine"], "last_report": t["ts"].isoformat(), "age_s": round(age),
                                              "stale": age > AGENT_STALE_S}
            jobs = c.execute("""SELECT j.booking_id, j.pid, j.state, j.started_at, j.from_cache FROM jobs j
                                JOIN bookings b ON b.id=j.booking_id WHERE b.resource_id=%s AND j.state='running'
                                ORDER BY j.started_at""", (r["resource_id"],)).fetchall()
            out.append({"resource": to_json(r), "active": [to_json(a) for a in active],
                        "next": [to_json(n) for n in nxt],
                        "shared_in_use_gb": float(sum((a["vram_budget_gb"] or 0) for a in active)),
                        "external_lock": lock, "gpu": gpu, "agent": agent, "jobs_running": [to_json(j) for j in jobs]})
        return out

    @app.get("/status")
    def status():
        with conn() as c:
            return {"utc": dt.datetime.now(dt.UTC).isoformat(), "resources": _status(c)}

    @app.get("/bookings")
    def list_bookings(resource: Optional[str] = None, state: Optional[str] = None,
                      from_: Optional[dt.datetime] = Query(None, alias="from"), to: Optional[dt.datetime] = None,
                      limit: int = 200):
        q = "SELECT * FROM bookings WHERE true"; args: list[Any] = []
        if resource: q += " AND resource_id=%s"; args.append(resource)
        if state: q += " AND state = ANY(%s)"; args.append(state.split(","))
        else: q += " AND state IN ('booked','active','template')"
        if from_: q += " AND upper(slot) > %s"; args.append(from_)
        if to: q += " AND lower(slot) < %s"; args.append(to)
        q += " ORDER BY lower(slot) LIMIT %s"; args.append(limit)
        with conn() as c:
            return [redact(to_json(b)) for b in c.execute(q, args).fetchall()]

    @app.get("/bookings/{bid}")
    def get_booking(bid: int):
        with conn() as c:
            b = c.execute("SELECT * FROM bookings WHERE id=%s", (bid,)).fetchone()
            if not b: raise HTTPException(404, "no such booking")
            l = c.execute("SELECT * FROM leases WHERE booking_id=%s", (bid,)).fetchone()
            j = c.execute("SELECT * FROM jobs WHERE booking_id=%s ORDER BY id DESC LIMIT 1", (bid,)).fetchone()
            return {"booking": redact(to_json(b)), "lease": to_json(l) if l else None, "job": to_json(j) if j else None}

    @app.post("/bookings", status_code=201)
    def create_booking(p: BookingIn, m: dict = Depends(auth)):
        start = (p.start or dt.datetime.now(dt.UTC)).astimezone(dt.UTC)
        if p.mode not in ("exclusive", "shared"): raise HTTPException(422, "mode must be exclusive|shared")
        if p.priority not in PRIORITY_RANK: raise HTTPException(422, f"priority must be one of {list(PRIORITY_RANK)}")
        if p.rrule:
            try: ZoneInfo(p.tz)
            except Exception: raise HTTPException(422, f"unknown tz {p.tz!r}")
        with conn() as c:
            try:
                with c.transaction():
                    row = insert_booking(c, m, p, start, state="template" if p.rrule else "booked")
                    emit(c, type_="BOOKING_CREATED", severity="green", from_machine=m["machine"],
                         resource_id=p.resource_id, booking_id=row["id"], to=["*"],
                         message=f"#{row['id']} {p.mode} {p.resource_id} {start:%m-%d %H:%MZ}+{p.duration_min}m "
                                 f"by {m['machine']}: {p.purpose[:60]}" + (f" (RRULE {p.rrule})" if p.rrule else ""))
            except (pgerr.ExclusionViolation, pgerr.RaiseException, pgerr.CheckViolation,
                    pgerr.ForeignKeyViolation) as e:
                raise conflict(e)
            c.commit()
            if p.rrule:
                rrule_expand(c); c.commit()
            return to_json(row)

    class EarliestIn(BookingIn):
        max_wait_min: int = Field(default=0, ge=0, le=7 * 24 * 60)

    @app.post("/bookings/earliest", status_code=201)
    def earliest(p: EarliestIn, m: dict = Depends(auth)):
        """Book the earliest slot that fits within [now, now+max_wait]. The DB is the oracle: candidate starts are
        tried in order and the first INSERT that survives the constraint+trigger wins. No duplicated conflict logic."""
        now = dt.datetime.now(dt.UTC).replace(microsecond=0)
        deadline = now + dt.timedelta(minutes=p.max_wait_min)
        with conn() as c:
            res = c.execute("SELECT members FROM resources WHERE resource_id=%s", (p.resource_id,)).fetchone()
            rids = [p.resource_id] + list((res or {}).get("members") or [])     # a pair waits for all its members
            ends = c.execute(
                """SELECT DISTINCT upper(slot) AS t FROM bookings
                    WHERE resource_id = ANY(%s) AND state IN ('booked','active') AND upper(slot) > %s AND upper(slot) <= %s
                    ORDER BY 1""", (rids, now, deadline)).fetchall()
            candidates = [now] + [e["t"] for e in ends]
            last_err = None
            for start in candidates:
                try:
                    with c.transaction():
                        row = insert_booking(c, m, p, start)
                        emit(c, type_="BOOKING_CREATED", severity="green", from_machine=m["machine"],
                             resource_id=p.resource_id, booking_id=row["id"],
                             message=f"#{row['id']} {p.mode} {p.resource_id} earliest-fit {start:%m-%d %H:%MZ}"
                                     f"+{p.duration_min}m by {m['machine']}: {p.purpose[:60]}")
                    c.commit()
                    return to_json(row) | {"waited_from": now.isoformat()}
                except (pgerr.ExclusionViolation, pgerr.RaiseException, pgerr.CheckViolation,
                        pgerr.ForeignKeyViolation) as e:
                    last_err = e
                    continue
            raise HTTPException(409, f"no {p.duration_min}-minute slot on {p.resource_id} opens within "
                                     f"{p.max_wait_min} min ({str(last_err).splitlines()[0] if last_err else 'busy'})")

    def _owned(c, bid: int, m: dict, host_ok: bool = False) -> dict:
        """The owner may act on its booking. With host_ok, so may the machine that HOSTS the resource (its agent
        runs the slot: heartbeat, job reports, extension, release-on-job-exit). Cancel stays owner-only."""
        b = c.execute("SELECT b.*, r.gpu_lock_path, r.host_machine FROM bookings b JOIN resources r USING (resource_id) WHERE b.id=%s",
                      (bid,)).fetchone()
        if not b: raise HTTPException(404, "no such booking")
        if b["owner_machine"] == m["machine"]: return b
        if host_ok and b["host_machine"] == m["machine"]: return b
        raise HTTPException(403, f"booking #{bid} belongs to {b['owner_machine']}" + (f" (host {b['host_machine']})" if host_ok else ""))

    def _host_resource(c, rid: str, m: dict) -> dict:
        r = c.execute("SELECT * FROM resources WHERE resource_id=%s", (rid,)).fetchone()
        if not r: raise HTTPException(404, f"no such resource {rid!r}")
        if r["host_machine"] != m["machine"]:
            raise HTTPException(403, f"{rid} is hosted by {r['host_machine']}, not {m['machine']}")
        return r

    @app.post("/bookings/{bid}/heartbeat")
    def heartbeat(bid: int, m: dict = Depends(auth)):
        with conn() as c:
            b = _owned(c, bid, m, host_ok=True)
            if b["state"] == "active":
                c.execute("UPDATE leases SET last_heartbeat=now(), heartbeats=heartbeats+1 WHERE booking_id=%s", (bid,))
                c.commit()
            return {"id": bid, "state": b["state"], "end_reason": b["end_reason"],
                    "ends": b["slot"].upper.isoformat(), "utc": dt.datetime.now(dt.UTC).isoformat()}

    @app.post("/bookings/{bid}/release")
    def release(bid: int, m: dict = Depends(auth)):
        with conn() as c:
            b = _owned(c, bid, m, host_ok=True)
            if b["state"] != "active": raise HTTPException(409, f"booking #{bid} is {b['state']}, not active")
            c.execute("SELECT set_config('gpub.actor', %s, false)", (m["machine"],))
            _end(c, b, "released", "holder_release")
            emit(c, type_="SLOT_RELEASED", severity="green", from_machine=m["machine"], resource_id=b["resource_id"],
                 booking_id=bid, message=f"#{bid} on {b['resource_id']} released early by {m['machine']}; "
                                         f"{(b['slot'].upper - dt.datetime.now(dt.UTC)).total_seconds()/60:.0f} min freed")
            c.commit()
            return {"id": bid, "state": "released"}

    @app.post("/bookings/{bid}/cancel")
    def cancel(bid: int, m: dict = Depends(auth)):
        with conn() as c:
            b = _owned(c, bid, m)
            if b["state"] not in ("booked", "template"):
                raise HTTPException(409, f"booking #{bid} is {b['state']}; use release for an active slot")
            c.execute("SELECT set_config('gpub.actor', %s, false)", (m["machine"],))
            c.execute("UPDATE bookings SET state='cancelled', end_reason='cancelled', ended_at=now() WHERE id=%s", (bid,))
            n = c.execute("UPDATE bookings SET state='cancelled', end_reason='template_cancelled', ended_at=now() "
                          "WHERE parent_id=%s AND state='booked' RETURNING id", (bid,)).fetchall()
            grp = [g["id"] for g in pair_group(c, b) if g["state"] == "booked"]   # the pair group goes too
            occ_members = [x["id"] for x in c.execute("SELECT id FROM bookings WHERE pair_id = ANY(%s) AND state='booked'",
                                                      ([x["id"] for x in n] or [0],)).fetchall()]
            if grp or occ_members:
                c.execute("UPDATE bookings SET state='cancelled', end_reason='cancelled_pair', ended_at=now() WHERE id = ANY(%s)",
                          (grp + occ_members,))
            emit(c, type_="BOOKING_CANCELLED", severity="green", from_machine=m["machine"], resource_id=b["resource_id"],
                 booking_id=bid, message=f"#{bid} cancelled by {m['machine']}" + (f" (+{len(n)} occurrences)" if n else ""))
            c.commit()
            return {"id": bid, "state": "cancelled", "occurrences_cancelled": len(n)}

    class ExtendIn(BaseModel):
        minutes: int = Field(gt=0, le=EXTEND_MAX_MIN)
        reason: str = ""

    @app.post("/bookings/{bid}/extend")
    def extend(bid: int, p: ExtendIn, m: dict = Depends(auth)):
        """Push the slot end out by N minutes. Only an ACTIVE booking; the EXCLUDE constraint and capacity trigger
        fire on UPDATE OF slot, so a refusal (409) means someone else holds that time. Nothing is moved for them."""
        with conn() as c:
            b = _owned(c, bid, m, host_ok=True)
            if b["state"] != "active": raise HTTPException(409, f"booking #{bid} is {b['state']}, not active")
            new_end = b["slot"].upper + dt.timedelta(minutes=p.minutes)
            try:
                with c.transaction():
                    c.execute("SELECT set_config('gpub.actor', %s, true)", (m["machine"],))
                    row = c.execute("""UPDATE bookings SET slot=tstzrange(lower(slot), %s, '[)'), extended_min=extended_min+%s
                                       WHERE id=%s RETURNING *""", (new_end, p.minutes, bid)).fetchone()
                    for g in pair_group(c, b):          # the group keeps one slot; a taken member time refuses all
                        c.execute("""UPDATE bookings SET slot=tstzrange(lower(slot), %s, '[)'), extended_min=extended_min+%s
                                     WHERE id=%s""", (new_end, p.minutes, g["id"]))
                    emit(c, type_="SLOT_EXTENDED", severity="green", from_machine=m["machine"], resource_id=b["resource_id"],
                         booking_id=bid, to=[b["owner_machine"], "*"],
                         message=f"#{bid} on {b['resource_id']} extended +{p.minutes} min to {new_end:%H:%MZ}"
                                 + (f" ({p.reason[:60]})" if p.reason else "") + f"; total +{row['extended_min']} min")
            except (pgerr.ExclusionViolation, pgerr.RaiseException, pgerr.CheckViolation) as e:
                raise conflict(e)
            c.commit()
            return to_json(row)

    class JobEvent(BaseModel):
        event: str                                   # started | exited
        pid: Optional[int] = None
        log_path: Optional[str] = None
        exit_code: Optional[int] = None
        from_cache: bool = False
        cmd: Optional[str] = None
        cwd: Optional[str] = None
        run_user: Optional[str] = None
        started_at: Optional[dt.datetime] = None
        ended_at: Optional[dt.datetime] = None
        note: str = ""

    @app.post("/bookings/{bid}/job")
    def job_event(bid: int, p: JobEvent, m: dict = Depends(auth)):
        """The HOST agent reports its job for this booking. started -> jobs row (running); exited -> exit code recorded,
        JOB_EXITED (green, rc 0) / JOB_FAILED (yellow). Idempotent per (booking, pid)."""
        with conn() as c:
            b = _owned(c, bid, m, host_ok=True)
            if b["host_machine"] != m["machine"]:
                raise HTTPException(403, f"only the host agent ({b['host_machine']}) reports jobs for {b['resource_id']}")
            job = b["job"] or {}
            cmd = p.cmd or (job.get("cmd") if isinstance(job.get("cmd"), str) else json.dumps(job.get("cmd")))
            if p.event == "started":
                existing = c.execute("SELECT id FROM jobs WHERE booking_id=%s AND pid=%s", (bid, p.pid)).fetchone()
                if existing:
                    return {"id": existing["id"], "booking_id": bid, "state": "running", "dup": True}
                row = c.execute("""INSERT INTO jobs (booking_id, host_machine, cmd, cwd, run_user, state, pid, log_path,
                                      started_at, from_cache, agent_machine, note)
                                   VALUES (%s,%s,%s,%s,%s,'running',%s,%s,%s,%s,%s,%s) RETURNING id""",
                                (bid, b["host_machine"], cmd or "", p.cwd or job.get("cwd"), p.run_user, p.pid, p.log_path,
                                 p.started_at or dt.datetime.now(dt.UTC), p.from_cache, m["machine"], p.note)).fetchone()
                emit(c, type_="JOB_STARTED", severity="green", from_machine=m["machine"], resource_id=b["resource_id"],
                     booking_id=bid, to=[b["owner_machine"], "*"],
                     message=f"#{bid} job started on {b['resource_id']} (pid {p.pid}"
                             + (", FROM CACHE: coordinator was unreachable" if p.from_cache else "") + f"): {(cmd or '')[:80]}")
                c.commit()
                return {"id": row["id"], "booking_id": bid, "state": "running"}
            if p.event == "exited":
                rc = p.exit_code
                state = "exited" if rc == 0 else ("killed" if (rc is not None and rc < 0) else "failed")
                row = c.execute("""UPDATE jobs SET state=%s, exit_code=%s, ended_at=%s, log_path=coalesce(%s, log_path),
                                      note=concat_ws(' | ', nullif(note,''), nullif(%s,''))
                                   WHERE booking_id=%s AND pid=%s AND state='running' RETURNING id""",
                                (state, rc, p.ended_at or dt.datetime.now(dt.UTC), p.log_path, p.note, bid, p.pid)).fetchone()
                if not row:   # a started report never arrived (agent offline the whole time): record the whole thing now
                    row = c.execute("""INSERT INTO jobs (booking_id, host_machine, cmd, cwd, run_user, state, pid, log_path,
                                          started_at, ended_at, exit_code, from_cache, agent_machine, note)
                                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                                    (bid, b["host_machine"], cmd or "", p.cwd or job.get("cwd"), p.run_user, state, p.pid,
                                     p.log_path, p.started_at or dt.datetime.now(dt.UTC), p.ended_at or dt.datetime.now(dt.UTC),
                                     rc, p.from_cache, m["machine"], p.note)).fetchone()
                emit(c, type_="JOB_EXITED" if rc == 0 else "JOB_FAILED", severity="green" if rc == 0 else "yellow",
                     from_machine=m["machine"], resource_id=b["resource_id"], booking_id=bid, to=[b["owner_machine"], "*"],
                     message=f"#{bid} job on {b['resource_id']} {state} rc={rc} (pid {p.pid})"
                             + (f"; log {p.log_path}" if p.log_path else "") + (f"; {p.note[:80]}" if p.note else ""))
                c.commit()
                return {"id": row["id"], "booking_id": bid, "state": state}
            raise HTTPException(422, "event must be started|exited")

    @app.get("/agents/{rid}/plan")
    def agent_plan(rid: str, days: int = 7, m: dict = Depends(auth)):
        """Everything the host agent needs to act alone for `days` if this coordinator dies: the resource row (lock path),
        live bookings with FULL job payloads, and the server clock for skew. The agent caches this verbatim."""
        now = dt.datetime.now(dt.UTC)
        with conn() as c:
            r = _host_resource(c, rid, m)
            rows = c.execute("""SELECT * FROM bookings WHERE resource_id=%s AND state IN ('booked','active')
                                  AND upper(slot) > %s AND lower(slot) < %s ORDER BY lower(slot)""",
                             (rid, now, now + dt.timedelta(days=days))).fetchall()
            return {"utc": now.isoformat(), "version": VERSION, "resource": to_json(r),
                    "bookings": [to_json(b) for b in rows]}

    class Report(BaseModel):
        gpu: Optional[dict] = None
        ollama: Optional[dict] = None
        agent: Optional[dict] = None

    @app.post("/agents/{rid}/report", status_code=201)
    def agent_report(rid: str, p: Report, m: dict = Depends(auth)):
        busy = bool((p.gpu or {}).get("apps")) or bool((p.ollama or {}).get("loaded"))
        with conn() as c:
            _host_resource(c, rid, m)
            c.execute("INSERT INTO telemetry (resource_id, machine, gpu, ollama, agent, busy) VALUES (%s,%s,%s,%s,%s,%s)",
                      (rid, m["machine"], json.dumps(p.gpu) if p.gpu is not None else None,
                       json.dumps(p.ollama) if p.ollama is not None else None,
                       json.dumps(p.agent) if p.agent is not None else None, busy))
            c.commit()
        return {"ok": True, "utc": dt.datetime.now(dt.UTC).isoformat()}

    class SignalIn(BaseModel):
        type: str
        severity: str = "yellow"
        message: str
        resource_id: Optional[str] = None
        booking_id: Optional[int] = None
        to: Optional[list[str]] = None
        dedupe: bool = True            # skip if an identical (type, booking_id, message) already exists

    @app.post("/signals", status_code=201)
    def post_signal(p: SignalIn, m: dict = Depends(auth)):
        if p.type not in AGENT_SIGNAL_TYPES:
            raise HTTPException(422, f"type must be one of {sorted(AGENT_SIGNAL_TYPES)}")
        if p.severity not in ("green", "yellow", "red"):
            raise HTTPException(422, "severity must be green|yellow|red")
        with conn() as c:
            if p.dedupe and c.execute("SELECT 1 FROM signals WHERE type=%s AND booking_id IS NOT DISTINCT FROM %s AND message=%s LIMIT 1",
                                      (p.type, p.booking_id, p.message)).fetchone():
                return {"signal_id": None, "dup": True}
            sid = emit(c, type_=p.type, severity=p.severity, from_machine=m["machine"], resource_id=p.resource_id,
                       booking_id=p.booking_id, to=p.to, message=p.message[:500])
            c.commit()
            return {"signal_id": sid}

    @app.get("/jobs")
    def list_jobs(resource: Optional[str] = None, state: Optional[str] = None, limit: int = 50):
        q = "SELECT j.*, b.resource_id, b.owner_machine, b.purpose FROM jobs j JOIN bookings b ON b.id=j.booking_id WHERE true"
        args: list[Any] = []
        if resource: q += " AND b.resource_id=%s"; args.append(resource)
        if state: q += " AND j.state = ANY(%s)"; args.append(state.split(","))
        q += " ORDER BY j.id DESC LIMIT %s"; args.append(min(limit, 500))
        with conn() as c:
            return [to_json(j) for j in c.execute(q, args).fetchall()]

    @app.get("/signals")
    def signals(limit: int = 50, machine: Optional[str] = None, severity: Optional[str] = None,
                since: Optional[dt.datetime] = None):
        q = "SELECT * FROM signals WHERE true"; args: list[Any] = []
        if machine: q += " AND (%s = ANY(to_machines) OR '*' = ANY(to_machines))"; args.append(machine)
        if severity: q += " AND severity = ANY(%s)"; args.append(severity.split(","))
        if since: q += " AND ts > %s"; args.append(since)
        q += " ORDER BY ts DESC LIMIT %s"; args.append(min(limit, 500))
        with conn() as c:
            return [to_json(s) for s in c.execute(q, args).fetchall()]

    @app.post("/signals/{sid}/ack")
    def ack(sid: str, m: dict = Depends(auth), note: str = ""):
        with conn() as c:
            if not c.execute("SELECT 1 FROM signals WHERE signal_id=%s", (sid,)).fetchone():
                raise HTTPException(404, "no such signal")
            c.execute("INSERT INTO signal_acks (signal_id, acked_by, note) VALUES (%s,%s,%s)", (sid, m["machine"], note))
            c.commit()
            return {"signal_id": sid, "acked_by": m["machine"]}

    # ---- read-only views for a browser on the private network (no token; the private bind IS the perimeter)
    def _report_text(c, days: int) -> str:
        now = dt.datetime.now(dt.UTC)
        since = now - dt.timedelta(days=days)
        lines = [f"# GPU Booker report — {now:%Y-%m-%d %H:%M}Z (last {days} d)", ""]
        for s in _status(c):
            r = s["resource"]
            lines.append(f"## {r['resource_id']}  ({r['model']}, {r['capacity_gb']:.0f} GB budget)")
            if s["active"]:
                for a in s["active"]:
                    lines.append(f"- ACTIVE #{a['id']} {a['mode']} {a['owner_machine']} until {a['end'][11:16]}Z — {a['purpose']}")
            else:
                lines.append("- idle (no active booking)")
            if s["external_lock"]:
                lines.append(f"- external lockfile: HELD by {s['external_lock'].get('holder')} since {s['external_lock'].get('started')}")
            for n in s["next"]:
                lines.append(f"- next #{n['id']} {n['mode']} {n['owner_machine']} {n['start'][5:16]}Z — {n['purpose']}")
            agg = c.execute(
                """SELECT state, end_reason, count(*) AS n,
                          coalesce(sum(extract(epoch from (least(upper(slot), coalesce(ended_at, upper(slot))) - lower(slot)))/3600),0) AS h
                     FROM bookings WHERE resource_id=%s AND lower(slot) >= %s AND state NOT IN ('template')
                    GROUP BY 1,2 ORDER BY 1,2""", (r["resource_id"], since)).fetchall()
            for a in agg:
                lines.append(f"- {a['state']}{'/' + a['end_reason'] if a['end_reason'] else ''}: {a['n']} booking(s), {float(a['h']):.1f} h")
            g, ag = s.get("gpu"), s.get("agent")
            if ag:
                ol = ", ".join(x.get("name", "?") for x in ((g or {}).get("ollama") or {}).get("loaded") or []) or "none"
                lines.append(f"- agent {ag.get('machine')} {ag.get('version','')} last report {ag['age_s']} s ago"
                             + (" (STALE)" if ag.get("stale") else "")
                             + f"; util {g.get('util_pct')}% temp {g.get('temp_c')}C mem {g.get('mem_used_gb')}/{g.get('mem_total_gb')} GB"
                               f" power {g.get('power_w')} W; compute apps {len(g.get('apps') or [])}; ollama loaded: {ol}")
            else:
                lines.append("- agent: none reporting (telemetry null until the gpub agent runs on the host)")
            for j in s.get("jobs_running") or []:
                lines.append(f"- job running for #{j['booking_id']} pid {j['pid']} since {str(j['started_at'])[11:16]}Z"
                             + (" (from cache)" if j.get("from_cache") else ""))
            jt = c.execute("""SELECT j.state, count(*) AS n FROM jobs j JOIN bookings b ON b.id=j.booking_id
                              WHERE b.resource_id=%s AND j.started_at >= %s GROUP BY 1 ORDER BY 1""", (r["resource_id"], since)).fetchall()
            if jt:
                lines.append("- jobs: " + ", ".join(f"{x['state']} {x['n']}" for x in jt))
            ub = c.execute("""SELECT count(*) AS n FROM telemetry t WHERE t.resource_id=%s AND t.ts >= %s AND t.busy
                              AND NOT EXISTS (SELECT 1 FROM bookings b WHERE b.resource_id=t.resource_id
                                              AND b.state IN ('active','released','expired','preempted') AND b.slot @> t.ts)""",
                           (r["resource_id"], since)).fetchone()["n"]
            tot = c.execute("SELECT count(*) AS n FROM telemetry WHERE resource_id=%s AND ts >= %s", (r["resource_id"], since)).fetchone()["n"]
            if tot:
                lines.append(f"- unbooked GPU use observed in {ub} of {tot} samples ({100*ub/tot:.0f}%): compute apps or loaded models with no booking")
            lines.append("")
        lines.append("## Signals (yellow/red, newest first)")
        for s in c.execute("SELECT * FROM signals WHERE severity IN ('yellow','red') AND ts >= %s ORDER BY ts DESC LIMIT 40",
                           (since,)).fetchall():
            lines.append(f"- {s['ts']:%m-%d %H:%M}Z {s['severity'].upper():6} {s['type']:18} {s['message']}")
        lines.append("")
        lines.append(f"_{VERSION} on {socket.gethostname()} · soft enforcement: nothing is ever killed; agents SIGTERM only their own job at slot end_")
        return "\n".join(lines)

    @app.get("/report", response_class=PlainTextResponse)
    def report(days: int = 7):
        with conn() as c:
            return _report_text(c, days)

    @app.get("/dash", response_class=HTMLResponse)
    def dash():
        # gpubdash, served same-origin so its read-only Live mode can call /resources, /bookings and /status
        page = HERE.parent / "viz" / "gpubdash.html"
        if not page.exists():
            raise HTTPException(404, "viz/gpubdash.html not found next to the gpub package")
        return HTMLResponse(page.read_text())

    @app.get("/", response_class=HTMLResponse)
    def index():
        import html
        with conn() as c:
            st = _status(c)
            sigs = c.execute("SELECT * FROM signals ORDER BY ts DESC LIMIT 50").fetchall()
            upcoming = c.execute("""SELECT * FROM bookings WHERE state IN ('booked','active')
                                    ORDER BY lower(slot) LIMIT 60""").fetchall()
        now = dt.datetime.now(dt.UTC)
        col = {"green": "#2e7d32", "yellow": "#b26a00", "red": "#c62828"}
        h = [f"<!doctype html><meta charset=utf-8><meta http-equiv=refresh content=30><title>GPU Booker</title>"
             "<style>body{font:14px/1.4 system-ui,sans-serif;margin:16px;background:#111;color:#ddd}"
             "h1,h2{font-weight:600}table{border-collapse:collapse;width:100%}td,th{padding:4px 8px;border-bottom:1px solid #333;text-align:left;vertical-align:top}"
             ".card{background:#1b1b1b;border:1px solid #333;border-radius:8px;padding:12px;margin:8px 0}"
             ".pri-critical{color:#ff6b6b}.pri-sweep{color:#ffd166}.pri-interactive{color:#8ecae6}.pri-background{color:#999}"
             "small{color:#888}</style>",
             f"<h1>GPU Booker <small>{now:%Y-%m-%d %H:%M:%S}Z · {VERSION} · <a href=/report style='color:#8ecae6'>text report</a></small></h1>"]
        for s in st:
            r = s["resource"]
            h.append(f"<div class=card><h2>{html.escape(r['resource_id'])} <small>{html.escape(r['model'] or '')} · "
                     f"{r['capacity_gb']:.0f} GB budget · shared in use {s['shared_in_use_gb']:.0f} GB</small></h2>")
            if s["active"]:
                for a in s["active"]:
                    h.append(f"<div>&#9654; <b>#{a['id']}</b> {a['mode']} <span class=pri-{a['priority']}>{a['priority']}</span> "
                             f"{html.escape(a['owner_machine'])} until {a['end'][11:16]}Z — {html.escape(a['purpose'])}"
                             f" <small>hb {a.get('heartbeats')} last {str(a.get('last_heartbeat') or '')[11:19]}</small></div>")
            else:
                h.append("<div>idle</div>")
            if s["external_lock"]:
                h.append(f"<div><small>external lockfile HELD by {html.escape(str(s['external_lock'].get('holder')))} "
                         f"since {html.escape(str(s['external_lock'].get('started')))}</small></div>")
            g, ag = s.get("gpu"), s.get("agent")
            if ag:
                ol = ", ".join(x.get("name", "?") for x in ((g or {}).get("ollama") or {}).get("loaded") or []) or "none"
                h.append(f"<div><small>agent {html.escape(str(ag.get('machine')))} {ag['age_s']} s ago"
                         f"{' <b style=color:#b26a00>STALE</b>' if ag.get('stale') else ''} · util {g.get('util_pct')}% · "
                         f"{g.get('temp_c')}°C · {g.get('power_w')} W · mem {g.get('mem_used_gb')}/{g.get('mem_total_gb')} GB · "
                         f"apps {len(g.get('apps') or [])} · ollama: {html.escape(ol)}</small></div>")
            else:
                h.append("<div><small>no agent reporting</small></div>")
            for j in s.get("jobs_running") or []:
                h.append(f"<div><small>job #{j['booking_id']} pid {j['pid']} running{' (from cache)' if j.get('from_cache') else ''}</small></div>")
            h.append("</div>")
        h.append("<div class=card><h2>Upcoming</h2><table><tr><th>#</th><th>start (UTC)</th><th>end</th><th>GPU</th><th>mode</th><th>pri</th><th>owner</th><th>state</th><th>purpose</th></tr>")
        for b in upcoming:
            h.append(f"<tr><td>{b['id']}</td><td>{b['slot'].lower:%m-%d %H:%M}</td><td>{b['slot'].upper:%m-%d %H:%M}</td>"
                     f"<td>{html.escape(b['resource_id'])}</td><td>{b['mode']}{' ' + str(int(b['vram_budget_gb'])) + 'G' if b['vram_budget_gb'] else ''}</td>"
                     f"<td class=pri-{b['priority']}>{b['priority']}</td><td>{html.escape(b['owner_machine'])}</td><td>{b['state']}</td>"
                     f"<td>{html.escape(b['purpose'])}</td></tr>")
        h.append("</table></div><div class=card><h2>Last 50 signals</h2><table>")
        for s in sigs:
            h.append(f"<tr><td><small>{s['ts']:%m-%d %H:%M:%S}</small></td><td style='color:{col[s['severity']]}'>{s['severity']}</td>"
                     f"<td>{html.escape(s['type'])}</td><td>{html.escape(s['from_machine'])}</td><td>{html.escape(s['message'])}</td></tr>")
        h.append("</table></div>")
        return "".join(h)

    return app


async def scheduler(db: str, tick_s: float):
    while True:
        try:
            await asyncio.to_thread(tick, db)
        except Exception:
            log.exception("scheduler tick failed")
        await asyncio.sleep(tick_s)


def serve(db: str, bind: str, port: int, tick_s: float) -> None:
    import uvicorn
    app = build_app(db)

    @app.on_event("startup")
    async def _start():
        asyncio.create_task(scheduler(db, tick_s))
        with connect(db) as c:
            emit(c, type_="COORDINATOR_UP", severity="green",
                 message=f"{VERSION} serving on {bind}:{port} (pid {os.getpid()}, tick {tick_s:g}s)")
            c.commit()

    uvicorn.run(app, host=bind, port=port, log_level="warning", proxy_headers=False)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=os.environ.get("GPUB_DB", "gpub"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    i = sub.add_parser("initdb"); i.add_argument("--hosts", type=Path, default=None,
                                                  help="optional JSON hosts file to fill machine IPs (examples/hosts.example.json)")
    i.add_argument("--lock-path", default="",
                   help="optional external lockfile wrapped for the seeded rtxbox-5090, e.g. /var/lock/gpub_gpu.lock ('' = none)")
    r = sub.add_parser("register"); r.add_argument("machine"); r.add_argument("--ip"); r.add_argument("--os")
    s = sub.add_parser("serve"); s.add_argument("--bind", default="127.0.0.1",
                                                help="bind address: a private LAN/VPN interface, never a public one")
    s.add_argument("--port", type=int, default=8766)
    s.add_argument("--tick", type=float, default=10.0)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if a.cmd == "initdb":
        ensure_db(a.db); apply_schema(a.db); seed(a.db, a.hosts, a.lock_path or None); return 0
    if a.cmd == "register":
        tok = register(a.db, a.machine, a.ip, a.os)
        print(f"TOKEN for {a.machine} (shown once, store 0600 at ~/.config/gpub/config.json):\n{tok}")
        return 0
    if a.cmd == "serve":
        serve(a.db, a.bind, a.port, a.tick); return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
