# GPUB — a GPU booker for homelabs

**[Live demo of the dashboard →](https://xgoldbug.github.io/gpub/)** (runs entirely in your browser; no server needed)

GPUB lets people, bots and scheduled jobs share a few GPUs without collisions. You book a GPU (or a group of
machines) for a time slot; Postgres guarantees that conflicting bookings can never both exist; a small agent on each GPU
host can run the booked job at slot start, keep the lease alive, and report telemetry.

It is three Python files and a SQL schema:

| Piece | File | Runs on | Needs |
|---|---|---|---|
| Coordinator (HTTP API + scheduler loop + web/text status) | `gpub/coordinator.py` | one always-on machine next to Postgres | `requirements.txt` |
| CLI (`gpub status / book / run / hold / ls / jobs / signals / ...`) | `gpub/cli.py` | every machine that books | stdlib only, Python 3.11+ |
| Host agent (telemetry, job runner, heartbeats, extensions, local plan cache) | `gpub/agent.py` | every machine that hosts a GPU | stdlib only, Python 3.11+ |
| Ledger | `sql/schema.sql`, `sql/migration_01_agents.sql`, `sql/migration_02_pairs.sql` | Postgres (16 tested) | `btree_gist` extension |

## Built for heterogeneous meshes

Homelabs are bought piecemeal: an RTX desktop one year, a couple of NVIDIA GB10 / DGX Spark-class boxes the next, an Apple
Silicon Mac in between. GPUB treats each of them as a *resource* with a configured memory budget, hosted by a registered
machine, and lets you declare *group resources* (a 2-box duo, a 4-box quad) that are booked as one unit. One queue, one
history, mixed hardware and operating systems. See **[docs/heterogeneous-mesh.md](docs/heterogeneous-mesh.md)** for the
resource model, what each kind of node gets today (and what it does not), and a worked mixed-mesh example
(`examples/mixed_mesh.sql`).

## Describe your mesh, then watch it

People buy homelab hardware piece by piece: an RTX 3090 here, a MacBook over Thunderbolt there, a pair of GB10 boxes later.
GPUB starts from a small **`mesh.json`** listing your machines, their GPUs (from hardware presets) and the groups booked
together. A guide written so a person or an LLM can fill it in is in **[docs/describe-your-mesh.md](docs/describe-your-mesh.md)**.

```bash
python tools/mesh_to_sql.py --presets                      # rtx-3090, rtx-5090, gb10, apple-silicon, ...
python tools/mesh_to_sql.py examples/mesh.example.json      # validates, prints register commands + SQL
```

**gpubdash** (`viz/gpubdash.html`) is a single-file dashboard with no build step and no external scripts. It has three modes:
- **Demo**: built-in scenarios that walk through the rules (a quad waiting for two duos, a refused group with no partial hold,
  shared VRAM adding up, a lease expiring).
- **Build my mesh**: a form with hardware presets that previews your topology and exports `mesh.json` plus setup commands.
  Nothing leaves the browser.
- **Live**: a read-only view of your coordinator, served by it at `http://<head node>:8766/dash`.

Its visual style is inspired by [sparkDash](https://github.com/MiaAI-Lab/sparkDash) by Mia's AI Lab (Apache-2.0).
No sparkDash code is included.

## Concepts

**Resources and machines.** A *resource* is one bookable compute unit (`rtxbox-5090`, `spark-a-gb10`) with a
`capacity_gb` budget that is *configured, never measured* (unified-memory devices report memory as N/A). A *machine* is a
registered host or client with its own bearer token and private IP. Adding a GPU means adding a row.

**Exclusive vs shared bookings, enforced by Postgres.** An exclusive booking owns the resource for its slot. A shared
booking declares a VRAM budget (`--shared 20G`). The database, not the API, prevents conflicts:
- an `EXCLUDE USING gist` constraint makes two live exclusive bookings on one resource impossible, whoever inserts them
  (even raw SQL);
- a `BEFORE` trigger refuses exclusive-over-shared and shared-over-exclusive overlaps and keeps the sum of shared budgets
  at every instant within `capacity_gb` (exact per-boundary peak, serialised per resource).
Slots are half-open `[start, end)`, so back-to-back bookings are fine. Nothing is ever deleted: `DELETE` is blocked by
trigger; ended bookings keep their final state (`released`, `expired`, `cancelled`, ...) and every change is written to
`audit_log`.

**Leases and heartbeats.** When a slot starts, the booking becomes `active` and gets a lease. If the booking has a grace
period (`--grace 5`, default 5 min; `--no-lease` = 0) the holder must heartbeat: `gpub run` and `gpub hold` do it for
interactive use, the host agent does it for jobs. No heartbeat within the grace period -> the lease expires (`no_show` or
`heartbeat_lost`), the slot is freed and `LEASE_EXPIRED` is signalled. Enforcement is *soft*: the coordinator never
kills anything.

**Earliest-fit queue.** `gpub book --asap --wait 30m` (or `gpub run`) asks for the earliest slot within the wait window.
The coordinator tries "now" and then each moment a live booking ends; the first insert that the database accepts wins.

**Recurring bookings (RRULE).** `--every "FREQ=WEEKLY;BYDAY=TU"` stores a template; the scheduler materialises
occurrences 14 days ahead **in the booker's IANA time zone**, so 02:00 stays 02:00 local across DST changes. An occurrence
that cannot be placed raises a `BOOKING_CONFLICT` signal. Cancelling the template cancels its future occurrences.

**Host agent and jobs.** A booking may carry a job (`--job 'cmd'` or a command after `--`). The agent on the GPU host
starts it at slot start *on that machine* (so `localhost:11434` in a bot means *that* GPU's Ollama), logs it, heartbeats
for it, asks for +15 min extensions while it runs (the database decides; a taken next slot means refusal), sends SIGTERM
once to the job's own process group if the slot ends without extension (`--on-slot-end continue` disables that; never
SIGKILL), and releases the slot early when the job exits. Before an exclusive slot it asks the local Ollama to unload
loaded models. A process on the GPU that is not the booked job during an exclusive slot raises `SQUATTER_DETECTED` and is
left alone. The agent caches a 7-day plan locally: if the coordinator is unreachable it still starts due jobs from the
cache and reports back when the coordinator returns (`COORDINATOR_DEGRADED`); clock skew > 5 s disables cache-driven starts.
Telemetry (nvidia-smi fields that exist, Ollama loaded models) goes to `/status`; a silent agent raises `AGENT_DOWN`.

**Group ("pair") resources.** A resource with `members` (e.g. a duo of two GB10 boxes over QSFP, or a quad of four on a
QSFP ring) is booked exclusive, as one, or not at all: the coordinator inserts one exclusive booking per member in the same
transaction, so any live booking on any member refuses the group with no partial hold. The group never preempts. The job
runs on the *driver* member (the member hosted by the group's `host_machine`), release/expiry/extension/cancel act on the
whole group, and earliest-fit waits for the busiest member. See `examples/resources_duo_quad.sql`.

**Optional external lockfile wrap.** If existing tools on a GPU host already serialise through a lockfile, set the
resource's `gpu_lock_path`. When a booking on it goes active the coordinator acquires that lockfile on the holder's behalf
(JSON body `{"holder": "gpub:<id>", "host", "pid", "started", "note"}`, created with `O_EXCL`) and releases it with the
lease. A lock held by someone else delays the slot (`SLOT_DELAYED`, retried every tick) rather than overriding it; locks
older than 6 h are treated as stale. The coordinator process must be able to reach the path (same host or a shared mount).

**Signals.** Every state change emits a signal (`SLOT_STARTED`, `LEASE_EXPIRED`, `JOB_FAILED`, ...) with severity
green/yellow/red and recipients; `gpub signals --follow` streams them and the web page shows the latest.

## Quick start

Requirements: Python 3.11+, PostgreSQL with the `btree_gist` extension available (it ships in the standard contrib
package), and a Postgres role for the user running the coordinator that may create a database (connections use libpq
defaults, so `PGHOST`/`PGUSER`/... work).

```bash
git clone <this repo> gpub && cd gpub
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

# 1. create the database, schema, migrations; seeds two example resources (rtxbox-5090, spark-a-gb10)
python gpub/coordinator.py --db gpub initdb            # optional: --hosts examples/hosts.example.json --lock-path /var/lock/gpub_gpu.lock
psql -d gpub -f examples/resources_duo_quad.sql        # optional: describe your own hardware (see examples/)

# 2. register every machine that will book or host; the token is printed ONCE (only its sha256 is stored)
python gpub/coordinator.py --db gpub register headnode --ip 127.0.0.1 --os linux
#    --ip must be the address the coordinator will SEE that machine's requests come from (its LAN/VPN IP)

# 3. serve (bind a private interface; 127.0.0.1 for a single-box trial)
python gpub/coordinator.py --db gpub serve --bind 127.0.0.1 --port 8766
#    browser: http://127.0.0.1:8766/  (status page)   http://127.0.0.1:8766/report  (text report)
```

On each client (`deploy/bootstrap.sh --config my.config.json` installs the CLI as `gpub`; add `--agent <resource>` on a
GPU host to install the agent as a systemd user service):

```bash
mkdir -p ~/.config/gpub && chmod 700 ~/.config/gpub
cp examples/config.example.json ~/.config/gpub/config.json && chmod 600 ~/.config/gpub/config.json   # edit url/machine/token
gpub time                                                   # clock-skew check
gpub status
gpub book --gpu rtxbox-5090 --at "tomorrow 02:00" --for 3h --purpose "render"
gpub book --gpu rtxbox-5090 --at "+10m" --for 1h --shared 12G --no-lease
gpub book --gpu spark-a-gb10 --asap --for 2h --wait 1h --job 'python3 ~/bots/batch.py'
gpub book --gpu spark-a-gb10 --at 02:00 --for 1h --every "FREQ=DAILY" --priority background --job 'bash ~/nightly.sh'
gpub run  --gpu rtxbox-5090 --for 30m --wait 15m -- python3 train.py     # book earliest, wait, run here, heartbeat, release
gpub ls --week ; gpub jobs ; gpub signals --follow
```

Host agent (on the machine that hosts the resource; reads the same `~/.config/gpub/config.json`):

```bash
python3 gpub/agent.py --resource spark-a-gb10 --poll 30     # --nvidia-smi PATH, --ollama URL, --log-dir DIR, --dry, --once
```

`deploy/` has a systemd unit for the coordinator (`/opt/gpub/venv`, user `gpub`), a user-unit template for the agent and
the bootstrap script. `examples/gpub-agent.launchd.plist` is an (untested) macOS LaunchAgent.

## API (summary)

Unauthenticated, read-only: `GET /time`, `/resources`, `/status`, `/bookings`, `/bookings/{id}`, `/jobs`, `/signals`,
`/report`, `/`. Public views redact `job.env` values (keys stay visible; `job.cmd` is visible — put secrets in `--env`).

Bearer token + matching source IP: `POST /bookings`, `/bookings/earliest`, `/bookings/{id}/heartbeat|release|cancel|extend|job`,
`/signals`, `/signals/{id}/ack`, `GET /agents/{resource}/plan`, `POST /agents/{resource}/report`. Only the resource's host
machine may use `/agents/{resource}/*` and report jobs; heartbeat/release/extend accept the owner or the host; cancel is
owner-only.

## Security notes

- **One bearer token per machine.** Printed once at `register`; only `sha256(token)` is stored. Re-running `register`
  rotates it. Keep `~/.config/gpub/config.json` at mode 600; never commit tokens (`.gitignore` covers `tokens/`, `*.config.json`).
- **Tokens are bound to a source IP.** A request is accepted only if it comes from the IP registered for that machine.
  This assumes a network where source addresses are meaningful (a private LAN or a WireGuard/Tailscale-style VPN). The
  coordinator ignores `X-Forwarded-For`, so do not put it behind a reverse proxy.
- **Bind to a private interface; never expose it publicly.** The read-only endpoints and the status page have no
  authentication, and the API speaks plain HTTP — rely on the VPN for encryption.
- **Soft enforcement.** The coordinator never kills anything. The agent only ever signals a job it started itself, with
  SIGTERM, once. Squatters are reported, not killed.
- Jobs run as the user that runs the agent, with that user's environment; anyone who can book a resource with a job can run
  commands on its host as that user. Register only machines you trust.

## Tests

`python tests/run_all.py` runs four gates (core, host agent with a fake nvidia-smi and a fake Ollama, group resources, mesh tooling + dashboard route),
about 10 minutes. **They DROP and recreate a database named `gpub_test`** and start a coordinator on `127.0.0.1:8767`
(fake Ollama on `127.0.0.1:18434`); run them where that is safe. Each gate exits non-zero on any failure.

Note on T7a (RRULE across DST): it checks the next three weekly 02:00 America/Chicago occurrences against their UTC
instants computed from today's date, so it exercises an actual DST transition only when one falls within the next three weeks.

## Limitations

No preemption (priority is recorded and shown, but it never bumps a booking); no topology discovery
(groups are declared by hand); GPU telemetry needs `nvidia-smi` (other vendors report no GPU fields); Postgres is required;
times are UTC on the wire and the coordinator host's clock is the reference.

## License

MIT, see `LICENSE`.
