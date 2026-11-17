#!/usr/bin/env python3
"""cli.py — GPUB (GPU Booker) CLI, installed as `gpub` on every machine; talks to the coordinator over HTTP.
Stdlib-only HTTP (no third-party packages required), job-carrying bookings (--job / trailing -- cmd), `gpub jobs`.

  gpub status                                  every GPU: holder, next bookings, external lockfile (if wrapped)
  gpub ls [--gpu X] [--week] [--all]           upcoming bookings (local time)
  gpub book --gpu X --at "tue 02:00" --for 3h [--shared 20G] [--priority sweep]
            [--every "FREQ=WEEKLY;BYDAY=TU"] [--purpose "..."] [--grace 5] [--no-lease]
  gpub book --gpu X --asap --for 2h [--wait 30m]     earliest-fit booking (the queue)
  gpub book --gpu spark-a-gb10 --at "02:00" --for 3h --every "FREQ=DAILY" --priority background --grace 5 \
            --job 'python3 ~/bots/some_bot.py --flag' [--cwd DIR] [--env K=V ...] [--on-slot-end term|continue] [--no-extend]
            the HOST AGENT runs the job ON the GPU machine at slot start (localhost:11434 there = that GPU; zero bot edits),
            heartbeats for it, extends the slot while it runs if nobody is waiting, releases when it exits.
            Alternatively give the command after `--` (exec'd directly, no shell).
  gpub jobs [--gpu X] [-n 20]                   job rows (state, rc, pid, log path on the host)
  gpub run  --gpu X --for 2h [--wait 30m] [--shared 20G] -- <cmd...>
            book earliest-fit, wait for the slot, run the command locally, heartbeat, release on exit
  gpub hold <id>                               heartbeat an active booking you are using interactively (Ctrl-C to stop)
  gpub release <id> | cancel <id>
  gpub signals [--follow] [--mine] [-n 50]
  gpub report [--days 7]                        the coordinator's text report
  gpub time                                     clock-skew check against the coordinator

Config: ~/.config/gpub/config.json  {"url": "http://127.0.0.1:8766", "machine": "rtxbox", "token": "<token>"}  (GPUB_CONFIG overrides)
No third-party package is required. python-dateutil (Ubuntu ships it) enables fuzzy --at phrases; without it use ISO / HH:MM / +30m.
"""
from __future__ import annotations

import argparse, datetime as dt, json, os, re, signal, subprocess, sys, threading, time, urllib.error, urllib.parse, urllib.request
import json as _json          # api() has a parameter named json
from pathlib import Path

CFG = Path(os.environ.get("GPUB_CONFIG", Path.home() / ".config" / "gpub" / "config.json"))
HEARTBEAT_S = 30
LOCAL_TZ = dt.datetime.now().astimezone().tzinfo


def cfg() -> dict:
    try:
        c = json.loads(CFG.read_text())
    except FileNotFoundError:
        sys.exit(f"gpub: no config at {CFG}. Run deploy/bootstrap.sh or create it (chmod 600): "
                 '{"url": "http://127.0.0.1:8766", "machine": "<name>", "token": "<token>"}')
    if (CFG.stat().st_mode & 0o077):
        print(f"gpub: WARNING {CFG} is group/world readable; chmod 600 it", file=sys.stderr)
    return c


def _http(method: str, url: str, headers: dict, params: dict | None = None, body: dict | None = None, timeout: float = 20):
    """(status, text) via urllib. Raises OSError family when unreachable."""
    if params:
        url += ("&" if "?" in url else "?") + urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"} | headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")


def api(method: str, path: str, auth: bool = True, params: dict | None = None, json: dict | None = None):
    c = cfg()
    headers = {"Authorization": f"Bearer {c['token']}"} if auth else {}
    try:
        status, text = _http(method, c["url"].rstrip("/") + path, headers, params, json)
    except (urllib.error.URLError, OSError) as e:
        sys.exit(f"gpub: coordinator unreachable at {c['url']} ({e.__class__.__name__}: {e}). "
                 "New bookings are impossible while it is down.")
    if status >= 400:
        try:
            detail = _json.loads(text).get("detail", text)
        except Exception:
            detail = text
        sys.exit(f"gpub: {status} {detail}")
    return _json.loads(text) if text else None


def local_tz_name() -> str:
    for src in (os.environ.get("TZ"), None):
        if src: return src
    try:
        return Path("/etc/timezone").read_text().strip()
    except Exception:
        pass
    try:
        tgt = os.readlink("/etc/localtime")
        return tgt.split("zoneinfo/", 1)[1]
    except Exception:
        return "UTC"


def fmt_local(iso: str) -> str:
    return dt.datetime.fromisoformat(iso).astimezone(LOCAL_TZ).strftime("%a %m-%d %H:%M")


def parse_duration(s: str) -> int:
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([mhd]?)\s*", s.lower())
    if not m: sys.exit(f"gpub: bad duration {s!r} (use 90m, 2h, 1.5h, 1d)")
    n, u = float(m.group(1)), m.group(2) or "m"
    return int(round(n * {"m": 1, "h": 60, "d": 1440}[u]))


def parse_when(s: str) -> dt.datetime:
    """Local-time phrases -> aware UTC datetime. 'now', '+30m', '02:00', 'tomorrow 02:00', 'tue 02:00', ISO."""
    try:
        from dateutil import parser as dp
    except ImportError:
        dp = None
    now = dt.datetime.now(LOCAL_TZ)
    s = s.strip()
    if s.lower() == "now":
        return now.astimezone(dt.UTC)
    if s.startswith("+"):
        return (now + dt.timedelta(minutes=parse_duration(s[1:]))).astimezone(dt.UTC)
    words = s.lower().split()
    tomorrow = "tomorrow" in words
    weekday = any(w[:3] in ("mon", "tue", "wed", "thu", "fri", "sat", "sun") for w in words)
    s2 = " ".join(w for w in words if w != "tomorrow")
    try:
        if dp is not None:
            t = dp.parse(s2, default=now.replace(second=0, microsecond=0))
        else:
            try:
                t = dt.datetime.fromisoformat(s2)
            except ValueError:
                hh, mm = s2.split(":")[:2]
                t = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
    except (ValueError, OverflowError) as e:
        sys.exit(f"gpub: cannot parse time {s!r}: {e} (install python-dateutil for phrases like 'tue 02:00')")
    if t.tzinfo is None:
        t = t.replace(tzinfo=LOCAL_TZ)
    if tomorrow:
        t += dt.timedelta(days=1)
    if t <= now:                       # a bare time or weekday already past -> roll forward
        t += dt.timedelta(days=7 if weekday else 1)
    return t.astimezone(dt.UTC)


def parse_gb(s: str) -> float:
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*[gG]?[bB]?\s*", s)
    if not m: sys.exit(f"gpub: bad VRAM budget {s!r} (use 20G)")
    return float(m.group(1))


# ------------------------------------------------------------------ commands
def cmd_status(a):
    s = api("GET", "/status", auth=False)
    for r in s["resources"]:
        res = r["resource"]
        print(f"{res['resource_id']:12} {res['model'] or '':28} budget {res['capacity_gb']:.0f} GB"
              + (f"  shared in use {r['shared_in_use_gb']:.0f} GB" if r["shared_in_use_gb"] else ""))
        for x in r["active"]:
            print(f"   ACTIVE #{x['id']} {x['mode']:9} {x['priority']:11} {x['owner_machine']:10} until {fmt_local(x['end'])}  {x['purpose']}")
        if not r["active"]:
            print("   idle")
        if r["external_lock"]:
            l = r["external_lock"]
            print(f"   external lockfile HELD by {l.get('holder')} on {l.get('host')} since {l.get('started')}")
        for n in r["next"]:
            print(f"   next   #{n['id']} {n['mode']:9} {n['priority']:11} {n['owner_machine']:10} {fmt_local(n['start'])} -> {fmt_local(n['end'])[-5:]}  {n['purpose']}")
    print(f"coordinator time {fmt_local(s['utc'])} ({LOCAL_TZ})")


def cmd_ls(a):
    params = {}
    if a.gpu: params["resource"] = a.gpu
    if a.all: params["state"] = "booked,active,template,released,expired,preempted,cancelled"
    now = dt.datetime.now(dt.UTC)
    params["from"] = (now - dt.timedelta(days=1 if a.all else 0)).isoformat()
    params["to"] = (now + dt.timedelta(days=7 if a.week else 30)).isoformat()
    rows = api("GET", "/bookings", auth=False, params=params)
    if not rows:
        print("no bookings in window"); return
    for b in rows:
        shared = f" {b['vram_budget_gb']:.0f}G" if b.get("vram_budget_gb") else ""
        rr = f"  RRULE {b['rrule']}" if b.get("rrule") else ""
        print(f"#{b['id']:<5} {b['state']:9} {b['resource_id']:12} {b['mode']}{shared:5} {b['priority']:11} "
              f"{b['owner_machine']:10} {fmt_local(b['start'])} -> {fmt_local(b['end'])[-5:]}  {b['purpose']}{rr}")


def _booking_payload(a) -> dict:
    p = {"resource_id": a.gpu, "mode": "shared" if a.shared else "exclusive",
         "vram_budget_gb": parse_gb(a.shared) if a.shared else None, "priority": a.priority,
         "duration_min": parse_duration(a.for_), "purpose": a.purpose or "", "tz": local_tz_name(),
         "grace_min": 0 if getattr(a, "no_lease", False) else a.grace, "enforce": "soft"}
    return p


def _job_payload(a) -> dict | None:
    argv = [x for x in (getattr(a, "argv", None) or []) if x != "--"] if getattr(a, "argv", None) else []
    if a.job and argv:
        sys.exit("gpub: give --job '<shell string>' OR a command after --, not both")
    if not a.job and not argv:
        if a.cwd or a.env or a.no_extend or a.on_slot_end != "term":
            sys.exit("gpub: --cwd/--env/--no-extend/--on-slot-end need a job (--job or -- cmd)")
        return None
    env = {}
    for kv in a.env or []:
        if "=" not in kv: sys.exit(f"gpub: --env wants K=V, got {kv!r}")
        k, v = kv.split("=", 1); env[k] = v
    j: dict = {"cmd": a.job if a.job else argv, "on_slot_end": a.on_slot_end, "extend": not a.no_extend,
               "release_on_exit": not a.no_release_on_exit}
    if a.cwd: j["cwd"] = a.cwd
    if env: j["env"] = env
    return j


def cmd_book(a):
    p = _booking_payload(a)
    job = _job_payload(a)
    if job:
        p["job"] = job
    if a.every:
        p["rrule"] = a.every
    if a.asap:
        p["max_wait_min"] = parse_duration(a.wait) if a.wait else 0
        b = api("POST", "/bookings/earliest", json=p)
    else:
        if not a.at: sys.exit("gpub: --at <time> or --asap is required")
        p["start"] = parse_when(a.at).isoformat()
        b = api("POST", "/bookings", json=p)
    kind = "template" if b["state"] == "template" else "booking"
    print(f"{kind} #{b['id']} {b['mode']} {b['resource_id']} {fmt_local(b['start'])} -> {fmt_local(b['end'])[-5:]} "
          f"({b['priority']}) {b['purpose']}" + (f"  RRULE {b['rrule']} in {b['tz']}" if b.get("rrule") else ""))
    if b.get("job"):
        print(f"job attached: the {b['resource_id']} host agent runs it at slot start and heartbeats for it. "
              f"On exit the log is copied to the agent's --log-dir ({b['resource_id']}/<id>_<date>.log).")
    elif b["state"] == "booked" and p["grace_min"]:
        print(f"heartbeat required within {p['grace_min']} min of slot start: use `gpub run` or `gpub hold {b['id']}` "
              f"(or book with --no-lease)")


def _heartbeat_loop(bid: int, stop: threading.Event, state: dict):
    while not stop.wait(HEARTBEAT_S):
        try:
            r = api("POST", f"/bookings/{bid}/heartbeat")
            state["last"] = r
            if r["state"] != "active":
                print(f"\ngpub: booking #{bid} is now {r['state']} ({r.get('end_reason')}). "
                      "You no longer hold the slot (soft enforcement: nothing is killed).", file=sys.stderr)
                stop.set()
        except SystemExit as e:
            print(f"\ngpub: heartbeat failed: {e}", file=sys.stderr)


def _wait_active(bid: int) -> dict:
    while True:
        r = api("GET", f"/bookings/{bid}", auth=False)["booking"]
        if r["state"] == "active":
            return r
        if r["state"] not in ("booked",):
            sys.exit(f"gpub: booking #{bid} became {r['state']} before it started")
        start = dt.datetime.fromisoformat(r["start"])
        wait = (start - dt.datetime.now(dt.UTC)).total_seconds()
        print(f"\rgpub: waiting for slot #{bid} ({fmt_local(r['start'])}, {max(wait,0)/60:.0f} min)   ", end="", file=sys.stderr)
        time.sleep(min(max(wait, 2), 15))


def cmd_run(a):
    if not a.argv: sys.exit("gpub run: give the command after --")
    p = _booking_payload(a)
    p["max_wait_min"] = parse_duration(a.wait) if a.wait else 0
    b = api("POST", "/bookings/earliest", json=p)
    bid = b["id"]
    print(f"gpub: booked #{bid} {b['mode']} {b['resource_id']} {fmt_local(b['start'])} -> {fmt_local(b['end'])[-5:]}", file=sys.stderr)
    _wait_active(bid)
    api("POST", f"/bookings/{bid}/heartbeat")
    print(f"\ngpub: slot #{bid} active; running: {' '.join(a.argv)}", file=sys.stderr)
    stop = threading.Event(); st: dict = {}
    t = threading.Thread(target=_heartbeat_loop, args=(bid, stop, st), daemon=True); t.start()
    rc = 1
    try:
        proc = subprocess.Popen(a.argv)
        def fwd(sig, _f):
            proc.send_signal(sig)
        signal.signal(signal.SIGINT, fwd); signal.signal(signal.SIGTERM, fwd)
        rc = proc.wait()
    finally:
        stop.set()
        try:
            cur = api("GET", f"/bookings/{bid}", auth=False)["booking"]
            if cur["state"] == "active":
                api("POST", f"/bookings/{bid}/release")
                print(f"gpub: released #{bid} (exit {rc})", file=sys.stderr)
        except SystemExit as e:
            print(f"gpub: release failed: {e}", file=sys.stderr)
    sys.exit(rc)


def cmd_hold(a):
    r = _wait_active(a.id)
    print(f"gpub: holding #{a.id} on {r['resource_id']} until {fmt_local(r['end'])}; Ctrl-C to stop heartbeating"
          + (" and release" if a.release_on_exit else ""), file=sys.stderr)
    api("POST", f"/bookings/{a.id}/heartbeat")
    stop = threading.Event(); st: dict = {}
    try:
        _heartbeat_loop(a.id, stop, st)
    except KeyboardInterrupt:
        pass
    if a.release_on_exit:
        api("POST", f"/bookings/{a.id}/release"); print(f"gpub: released #{a.id}")


def cmd_release(a):
    print(api("POST", f"/bookings/{a.id}/release"))


def cmd_cancel(a):
    print(api("POST", f"/bookings/{a.id}/cancel"))


def cmd_signals(a):
    params = {"limit": a.n}
    if a.mine: params["machine"] = cfg()["machine"]
    seen = set()
    while True:
        rows = api("GET", "/signals", auth=False, params=params)
        for s in reversed(rows):
            if s["signal_id"] in seen: continue
            seen.add(s["signal_id"])
            flag = {"green": " ", "yellow": "!", "red": "!!"}[s["severity"]]
            print(f"{fmt_local(s['ts'])} {flag:2} {s['type']:18} {s['from_machine']:12} {s['message']}")
        if not a.follow: break
        params["limit"] = 20
        time.sleep(10)


def cmd_report(a):
    try:
        print(_http("GET", cfg()["url"].rstrip("/") + "/report", {}, {"days": a.days})[1])
    except (urllib.error.URLError, OSError) as e:
        sys.exit(f"gpub: coordinator unreachable ({e})")


def cmd_jobs(a):
    params = {"limit": a.n}
    if a.gpu: params["resource"] = a.gpu
    rows = api("GET", "/jobs", auth=False, params=params)
    if not rows:
        print("no jobs recorded"); return
    for j in rows:
        rc = "" if j["exit_code"] is None else f" rc={j['exit_code']}"
        print(f"#{j['booking_id']:<5} {j['state']:8}{rc:8} {j['resource_id']:12} {j['owner_machine']:10} pid {j['pid'] or '-':<8} "
              f"{fmt_local(j['started_at']) if j['started_at'] else '':16} "
              f"{('-> ' + fmt_local(j['ended_at'])[-5:]) if j.get('ended_at') else '':9} "
              f"{'cache ' if j.get('from_cache') else ''}{(j['cmd'] or '')[:50]}  {j['log_path'] or ''}")


def cmd_time(a):
    t0 = time.time(); r = api("GET", "/time", auth=False); rtt = time.time() - t0
    srv = dt.datetime.fromisoformat(r["utc"]); skew = (srv - dt.datetime.now(dt.UTC)).total_seconds() + rtt / 2
    verdict = "OK" if abs(skew) <= 5 else "SKEW > 5 s: do not enforce slot boundaries from this host"
    print(f"coordinator {r['version']} skew {skew:+.3f} s (rtt {rtt*1000:.0f} ms) -> {verdict}")
    return 0 if abs(skew) <= 5 else 4


def main():
    ap = argparse.ArgumentParser(prog="gpub", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status").set_defaults(f=cmd_status)
    s = sub.add_parser("ls"); s.add_argument("--gpu"); s.add_argument("--week", action="store_true"); s.add_argument("--all", action="store_true"); s.set_defaults(f=cmd_ls)

    def booking_args(s, asap_ok=True):
        s.add_argument("--gpu", required=True); s.add_argument("--for", dest="for_", required=True, help="90m, 2h, 1d")
        s.add_argument("--shared", metavar="GB", help="shared mode with this VRAM budget, e.g. 20G")
        s.add_argument("--priority", default="interactive", choices=["critical", "sweep", "interactive", "background"])
        s.add_argument("--purpose", default=""); s.add_argument("--grace", type=int, default=5, help="minutes without heartbeat before the lease expires")
        s.add_argument("--wait", help="earliest-fit: wait up to this long for a slot (30m, 2h)")
    s = sub.add_parser("book"); booking_args(s)
    s.add_argument("--at", help="start: 'now', '+30m', '02:00', 'tomorrow 02:00', 'tue 02:00', ISO")
    s.add_argument("--asap", action="store_true", help="earliest-fit within --wait")
    s.add_argument("--every", help="RFC 5545 RRULE, e.g. FREQ=WEEKLY;BYDAY=MO,WE (expands in your local tz)")
    s.add_argument("--no-lease", action="store_true", help="no heartbeat required; slot runs to its end")
    s.add_argument("--job", help="shell command the HOST agent runs on the GPU machine at slot start")
    s.add_argument("--cwd"); s.add_argument("--env", action="append", metavar="K=V")
    s.add_argument("--on-slot-end", default="term", choices=["term", "continue"],
                   help="term (default): SIGTERM the job's own process group if the slot ends and cannot be extended; continue: never signal it")
    s.add_argument("--no-extend", action="store_true", help="do not ask for +15 min extensions while the job runs")
    s.add_argument("--no-release-on-exit", action="store_true", help="keep the slot after the job exits")
    s.add_argument("argv", nargs=argparse.REMAINDER, help="alternative to --job: the command after --")
    s.set_defaults(f=cmd_book)
    s = sub.add_parser("jobs"); s.add_argument("--gpu"); s.add_argument("-n", type=int, default=20); s.set_defaults(f=cmd_jobs)
    s = sub.add_parser("run"); booking_args(s); s.add_argument("argv", nargs=argparse.REMAINDER); s.set_defaults(f=cmd_run)
    s = sub.add_parser("hold"); s.add_argument("id", type=int); s.add_argument("--release-on-exit", action="store_true"); s.set_defaults(f=cmd_hold)
    s = sub.add_parser("release"); s.add_argument("id", type=int); s.set_defaults(f=cmd_release)
    s = sub.add_parser("cancel"); s.add_argument("id", type=int); s.set_defaults(f=cmd_cancel)
    s = sub.add_parser("signals"); s.add_argument("--follow", action="store_true"); s.add_argument("--mine", action="store_true"); s.add_argument("-n", type=int, default=50); s.set_defaults(f=cmd_signals)
    s = sub.add_parser("report"); s.add_argument("--days", type=int, default=7); s.set_defaults(f=cmd_report)
    sub.add_parser("time").set_defaults(f=cmd_time)
    a = ap.parse_args()
    if a.cmd in ("run", "book") and a.argv[:1] == ["--"]:
        a.argv = a.argv[1:]
    rc = a.f(a)
    sys.exit(rc or 0)


if __name__ == "__main__":
    main()
