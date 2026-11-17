# GPUB on a heterogeneous mesh

GPUB is a scheduler for the kind of compute pool a homelab actually ends up with: a handful of machines bought one at a
time, each different. This page explains the resource model, what each kind of node gets **today** (verified against
`gpub/coordinator.py` and `gpub/agent.py`), how bookings compose across singles and groups, and the honest limitations.

Statements marked *(tested)* are exercised by `tests/run_all.py` (on Linux, with a fake `nvidia-smi` and a fake
Ollama). Statements marked *(not exercised by the tests)* follow from reading the code but have not been run by the suite.

## Why homelabs end up heterogeneous

Nobody buys a homelab cluster in one purchase order. A typical history: a gaming desktop with a consumer RTX card
(often running Linux under WSL2 because the box is also a Windows desktop), later one or more NVIDIA GB10 /
DGX Spark-class boxes for large-memory inference, an Apple Silicon Mac with lots of unified memory, maybe a second one of
something when a deal comes along. Different vendors, operating systems, memory models (discrete VRAM vs unified memory),
interconnects (QSFP between GB10 boxes, Thunderbolt between Macs) and tooling.

The usual coordination methods break down as soon as more than one person or bot uses the machines:

- **One queue per box** (a lockfile, a tmux session, a per-machine job runner) cannot express "I need *both* of these
  boxes from 02:00 to 05:00", has no view across boxes, and every tool needs to know which box's queue to use.
- **A chat channel** ("taking the 5090 for an hour") has no enforcement: two bookings collide, a forgotten claim leaves
  a GPU idle all night, and there is no history of who used what.
- **Nothing at all** means a nightly batch job and an interactive session load two large models into the same memory
  and one of them dies.

GPUB replaces these with one ledger: every booking for every machine lives in one Postgres database, conflicts are
refused by the database itself, idle leases expire, and the history (bookings, jobs, signals, audit log) is kept.

## The resource model

**Machine.** Any computer that books or hosts: it is registered with `coordinator.py register <machine> --ip <IP>`,
which prints a per-machine bearer token once (only its sha256 is stored) and records the private IP the machine's requests
must come from. A request is accepted only if token *and* source IP match *(tested: T10)*.

**Resource.** One bookable compute unit, hosted by one machine (`resources.host_machine`), with a `capacity_gb` budget.
The budget is **configured, never measured**: for a discrete GPU it is the VRAM; for unified-memory systems (GB10, Apple
Silicon) it is the share of system memory you are willing to hand out, leaving headroom for the OS. GB10's `nvidia-smi`
reports memory as `[N/A]`; the agent turns that into `null` rather than guessing *(tested with a GB10-shaped fake
nvidia-smi)*. Exclusive bookings take the whole resource; shared bookings declare a budget and the database keeps the sum
of overlapping shared budgets within `capacity_gb` at every instant *(tested: T2)*.

**Group resource.** A resource whose `members` column lists other resources, e.g. two GB10 boxes cabled back to back
(a *duo*) or four on a QSFP ring (a *quad*). Booking a group:

- is exclusive-only (a shared booking on a group is refused with 422) *(tested: P8)*;
- inserts, in the same transaction, one exclusive booking per member; if any member already has a live booking in that
  window (exclusive *or* shared) the database refuses the member row and the whole transaction rolls back — 409, no
  partial hold *(tested: P2, with a 2-member duo)*;
- never preempts: priorities never bump a booking, so a quad waits for, or is refused by, any single or duo on its
  members, and while a quad is booked every single and duo on its members is refused;
- acts as one afterwards: release, expiry, slot end, cancel and extension apply to the whole group; an extension is
  refused if any member's next slot is taken *(tested: P3, P4, P5)*; recurring (RRULE) group bookings expand with their
  members *(tested: P6)*.

The quad and the second duo in `examples/resources_duo_quad.sql` use exactly the same code path as the tested duo, with
four or two members *(4-member groups not exercised by the tests)*.

**Driver member.** A job attached to a group booking runs on exactly one member: the member whose host machine equals the
group's `host_machine` (or the first member if none does). That member's agent starts the job and heartbeats for it;
the other members are held idle with no job and no heartbeat requirement *(tested: P1)*. GPUB does not start anything on
the other members: a multi-node job (distributed inference across a duo, say) must reach them itself (ssh, MPI,
containers). Note that the agents on the non-driver members still run their squatter watch: compute processes started
there by the driver's job show up as `SQUATTER_DETECTED` (yellow, never killed) on their job-less exclusive hold.

## Node kinds supported today

The coordinator is one Python process next to Postgres; the CLI and agent are standard-library-only Python 3.11+ scripts.
The agent uses POSIX process groups (`start_new_session`, `os.killpg`, `os.getsid`) and runs string jobs with `bash -c`,
so it runs on Linux (including WSL2) and macOS; native Windows is not supported (use WSL2).

### (a) NVIDIA GB10 / DGX Spark-class Linux ARM boxes — single, duo, quad

- **Single:** one resource per box, e.g. `spark-a-gb10` with a 110 GB budget of ~121 GB unified memory.
- **Telemetry:** the agent runs `nvidia-smi --query-gpu=utilization.gpu,temperature.gpu,memory.used,memory.total,power.draw`
  and `--query-compute-apps`; fields reported as `[N/A]` (memory on GB10) become `null`. Utilisation, temperature, power
  and the compute-process list are reported when the driver provides them *(tested with a fake nvidia-smi producing
  GB10-shaped output; not tested against real hardware by the suite)*.
- **Squatter watch:** during an exclusive active slot, a compute process listed by `nvidia-smi` that is not in the job's
  session (and is not Ollama) raises `SQUATTER_DETECTED`; nothing is killed *(tested: T4)*.
- **Duo and quad:** declare group resources over the singles (`examples/resources_duo_quad.sql`). GPUB does not know or
  check the QSFP cabling; the group is whatever members you list.

### (b) Apple Silicon Macs on macOS — single and Mac pair

*(None of this is exercised by the tests, which run on Linux.)*

- **Agent:** runs under a launchd LaunchAgent (`examples/gpub-agent.launchd.plist`), with a Python 3.11+ interpreter
  (the macOS system `python3` is older; the scripts use `datetime.UTC`).
- **Telemetry without nvidia-smi:** the agent looks for `nvidia-smi` (explicit path, `PATH`, `/usr/lib/wsl/lib/nvidia-smi`,
  `/usr/bin/nvidia-smi`); on a Mac it finds none and reports `gpu: {"error": "nvidia-smi not found"}`. There is no GPU
  utilisation, temperature, power, memory or per-process data, and therefore **no squatter detection** on a Mac. The agent
  still reports its own state (version, uptime, skew, running jobs) and the local Ollama's loaded models, so `/status`,
  `AGENT_DOWN`/`AGENT_UP` and the "unbooked use" count (from Ollama models loaded outside a booking) still work.
- **Unified memory:** express the budget as a share of system memory, e.g. `capacity_gb = 96` on a 128 GB Mac.
- **Everything else is the same code:** bookings, leases, job start at slot start, heartbeats, extensions, SIGTERM at slot
  end, Ollama unload before an exclusive slot, the offline plan cache.
- **Mac pair:** two Macs (e.g. linked over Thunderbolt) can be declared as a group resource exactly like a GB10 duo
  (`mac-duo` in `examples/mixed_mesh.sql`); the driver Mac runs the job.
- `deploy/bootstrap.sh` installs the CLI on macOS too, but installs the agent only where `systemctl` exists; on a Mac it
  prints the command to run under your service manager instead.

### (c) Consumer RTX desktop (e.g. RTX 5090), Linux or WSL2

- **Single resource** with the card's VRAM as budget (`rtxbox-5090`, 32 GB). Shared bookings split it by budget, e.g. two
  12 GB jobs side by side *(tested: T2)*.
- **WSL2:** the agent also looks for `nvidia-smi` at `/usr/lib/wsl/lib/nvidia-smi`, and `bootstrap.sh` does the same.
  From inside WSL, per-process GPU attribution is generally unavailable, so expect an empty or partial compute-process
  list and correspondingly weak squatter detection *(not exercised by the tests)*.
- **Optional external lockfile wrap** for legacy tools that already serialise through a lockfile: set
  `resources.gpu_lock_path`. When a booking goes active the coordinator acquires the lockfile on the holder's behalf
  (`O_EXCL`, JSON `{"holder": "gpub:<id>", ...}`) and releases it when the lease ends; a lock held by another holder
  delays the slot with `SLOT_DELAYED` instead of overriding it; locks older than 6 h are reclaimed as stale *(tested:
  acquire, delay, release, release on expiry)*. When the coordinator is unreachable and the agent starts a job from its
  cache, the agent takes the same lockfile itself (`LANE_BLOCKED` if someone else holds it) *(cache path not tested with a
  lockfile)*. The coordinator opens the path directly, so it has to be reachable from the coordinator process (same host
  or a shared mount).

## Worked example: a mixed mesh

`examples/mixed_mesh.sql` describes:

| Machine | Role | Resources |
|---|---|---|
| `headnode` | Postgres + coordinator; books from the CLI | — |
| `rtxbox` | RTX desktop (Linux/WSL2) | `rtxbox-5090` (32 GB) |
| `spark-a` .. `spark-d` | GB10 boxes on a QSFP ring | `spark-a-gb10`, `spark-b-gx10`, `spark-c-gb10`, `spark-d-gx10` (110 GB each); `gb10-duo` (a+b), `gb10-duo-2` (c+d), `gb10-quad` (all four) |
| `mac-a`, `mac-b` | Apple Silicon Macs | `mac-a-gpu`, `mac-b-gpu` (96 GB each); `mac-duo` (both) |

```bash
python gpub/coordinator.py --db gpub initdb
psql -d gpub -f examples/mixed_mesh.sql
for m in headnode rtxbox spark-a spark-b spark-c spark-d mac-a mac-b; do
  python gpub/coordinator.py --db gpub register "$m" --ip <that machine's private IP>     # store each token safely
done
# one agent per resource, on its host:  agent.py --resource spark-a-gb10   (on spark-a), ... --resource mac-a-gpu (on mac-a)
```

(`initdb` also seeds `rtxbox-5090`, `spark-a-gb10` and machine rows for `headnode`, `workstation`, `rtxbox`, `spark-a`;
the example uses `ON CONFLICT DO NOTHING`, so applying both is fine. Do not run an agent for a group resource itself:
the group's own booking row also carries the job, so an agent for `gb10-duo` would start it a second time; the driver
member's agent is the one that runs it.)

## How booking modes compose

| You book | What is held | Refused when |
|---|---|---|
| a single, exclusive | that resource | any live booking overlaps on it |
| a single, shared `--shared 20G` | 20 GB of its budget | an exclusive overlaps, or shared budgets would exceed `capacity_gb` at any instant |
| a duo | both members, exclusive | any live booking on either member overlaps |
| the quad | all four members, exclusive | any live booking (single or duo) on any member overlaps |

- **Earliest-fit across a group** (`gpub book --asap --wait 2h --gpu gb10-quad`): candidate start times are "now" and the end
  of every live booking on the group *and its members*; the first one the database accepts wins, so the group effectively
  waits for its busiest member *(tested: P7)*.
- **Duos inside a quad:** `gb10-duo` and `gb10-duo-2` have disjoint members, so they can run at the same time; either one
  blocks the quad for its window, and the quad blocks both.
- **Recurring group bookings** expand with their members; an occurrence that collides is skipped and signalled as
  `BOOKING_CONFLICT`.

## Practical tips

- **Budgets as fractions of memory.** On unified-memory systems leave OS headroom (e.g. 110 of ~121 GB, 96 of 128 GB). For
  shared use, size `--shared` budgets to what the model really needs; the database only checks the arithmetic, not real usage.
- **Clock skew.** Slot boundaries are the coordinator's clock. `gpub time` exits 4 when skew exceeds 5 s. The agent
  measures skew at start and on reconnect; above 5 s it signals `CLOCK_SKEW` and refuses cache-driven job starts and
  offline self-extensions until fixed. Run NTP (or chrony) everywhere.
- **Keep the coordinator private.** Bind it to a LAN or VPN address (WireGuard/Tailscale-style), not a public interface;
  read endpoints are unauthenticated and the API is plain HTTP. Register each machine with the address the coordinator will
  see on that network.
- **Jobs and Ollama.** A job runs on the GPU host, so `localhost` services there (e.g. Ollama on `:11434`) are that GPU's.
  Before an exclusive slot the agent asks the local Ollama to unload loaded models (`keep_alive: 0`) *(tested: T8)*.

## Honest limitations

- **No preemption.** Priority never bumps a booking; a group waits or is refused.
- **No topology discovery or recabling.** Groups are declared by hand; GPUB does not inspect QSFP/Thunderbolt links, check
  that members can talk to each other, or start anything on non-driver members.
- **Telemetry depends on vendor tools.** GPU fields and squatter detection need `nvidia-smi`; Macs and non-NVIDIA GPUs get
  agent/Ollama telemetry only. WSL2 limits per-process attribution.
- **Soft enforcement only.** Nothing stops a process from using a GPU outside a booking; it is reported, not prevented.
- **Postgres required** (with `btree_gist`); the conflict rules live in the database.
- **Single coordinator.** If it is down no new bookings can be made; agents keep executing their cached 7-day plan.
