-- mixed_mesh.sql — a worked example of a HETEROGENEOUS mesh in one ledger (see docs/heterogeneous-mesh.md).
-- Apply AFTER `coordinator.py initdb`:   psql -d gpub -f examples/mixed_mesh.sql
-- Additive and idempotent. Generic names; replace models, budgets and hosts with yours. Then, for every machine
-- that will book or host: `coordinator.py register <machine> --ip <its private LAN/VPN IP>` (sets token + IP).
--
--   headnode      runs Postgres + the coordinator; books via the CLI; hosts no GPU here
--   rtxbox        consumer RTX desktop (Linux or WSL2), resource rtxbox-5090, optional external lockfile wrap
--   spark-a..d    four NVIDIA GB10 / DGX Spark-class Linux ARM boxes: singles, two duos, one quad (QSFP ring)
--   mac-a, mac-b  Apple Silicon Macs (macOS): singles and a 2-Mac group (e.g. over Thunderbolt)
--
-- Budgets are CONFIGURED GB, never read live. Discrete GPU: its VRAM. Unified memory (GB10, Apple Silicon): a share of
-- system memory that leaves the OS headroom. Group resources are exclusive-only; their capacity_gb is informational.

INSERT INTO machines (machine, os) VALUES
  ('headnode', 'linux'), ('rtxbox', 'linux-wsl2'),
  ('spark-a', 'linux-arm64'), ('spark-b', 'linux-arm64'), ('spark-c', 'linux-arm64'), ('spark-d', 'linux-arm64'),
  ('mac-a', 'macos'), ('mac-b', 'macos')
ON CONFLICT (machine) DO NOTHING;

-- Consumer RTX desktop. To make legacy tools that already use a lockfile and GPUB respect each other, set
-- gpu_lock_path (see README "Optional external lockfile wrap"), e.g.:
--   UPDATE resources SET gpu_lock_path = '/var/lock/gpub_gpu.lock' WHERE resource_id = 'rtxbox-5090';
-- The path is opened by the COORDINATOR process (and by the host agent only when running from its cache), so it must be
-- a path both can reach and the legacy tools use: in practice coordinator, agent and tools on one host or a shared mount.
INSERT INTO resources (resource_id, host_machine, model, capacity_gb, gpu_lock_path, notes) VALUES
  ('rtxbox-5090', 'rtxbox', 'NVIDIA GeForce RTX 5090', 32, NULL, '32 GB VRAM; shared bookings split it by budget')
ON CONFLICT (resource_id) DO NOTHING;

-- GB10 singles, duos, quad (same as examples/resources_duo_quad.sql; the rule is explained there).
INSERT INTO resources (resource_id, host_machine, model, capacity_gb, gpu_lock_path, notes) VALUES
  ('spark-a-gb10', 'spark-a', 'NVIDIA GB10 (DGX Spark-class)', 110, NULL, 'single; 110 GB budget of ~121 GB unified memory'),
  ('spark-b-gx10', 'spark-b', 'NVIDIA GB10 (DGX Spark-class)', 110, NULL, 'single; 110 GB budget of ~121 GB unified memory'),
  ('spark-c-gb10', 'spark-c', 'NVIDIA GB10 (DGX Spark-class)', 110, NULL, 'single; 110 GB budget of ~121 GB unified memory'),
  ('spark-d-gx10', 'spark-d', 'NVIDIA GB10 (DGX Spark-class)', 110, NULL, 'single; 110 GB budget of ~121 GB unified memory')
ON CONFLICT (resource_id) DO NOTHING;
INSERT INTO resources (resource_id, host_machine, model, capacity_gb, gpu_lock_path, members, notes) VALUES
  ('gb10-duo',   'spark-a', 'DUO: 2x GB10 over QSFP', 220, NULL, ARRAY['spark-a-gb10', 'spark-b-gx10'], 'driver spark-a'),
  ('gb10-duo-2', 'spark-c', 'DUO: 2x GB10 over QSFP', 220, NULL, ARRAY['spark-c-gb10', 'spark-d-gx10'], 'driver spark-c'),
  ('gb10-quad',  'spark-a', 'QUAD: 4x GB10 on a QSFP ring', 440, NULL,
   ARRAY['spark-a-gb10', 'spark-b-gx10', 'spark-c-gb10', 'spark-d-gx10'], 'all four exclusive; never preempts; driver spark-a')
ON CONFLICT (resource_id) DO NOTHING;

-- Apple Silicon Macs. No nvidia-smi on macOS: the agent reports gpu.error and no per-process GPU apps, so squatter
-- detection does not fire there; bookings, jobs, heartbeats, extensions and Ollama yield work the same.
-- Example: 128 GB unified memory -> 96 GB budget.
INSERT INTO resources (resource_id, host_machine, model, capacity_gb, gpu_lock_path, notes) VALUES
  ('mac-a-gpu', 'mac-a', 'Apple Silicon (unified memory)', 96, NULL, 'single; 96 GB budget of 128 GB unified memory'),
  ('mac-b-gpu', 'mac-b', 'Apple Silicon (unified memory)', 96, NULL, 'single; 96 GB budget of 128 GB unified memory')
ON CONFLICT (resource_id) DO NOTHING;
INSERT INTO resources (resource_id, host_machine, model, capacity_gb, gpu_lock_path, members, notes) VALUES
  ('mac-duo', 'mac-a', 'DUO: 2x Apple Silicon (e.g. over Thunderbolt)', 192, NULL, ARRAY['mac-a-gpu', 'mac-b-gpu'],
   'One booking holds both Macs exclusive; job runs on mac-a (driver)')
ON CONFLICT (resource_id) DO NOTHING;
