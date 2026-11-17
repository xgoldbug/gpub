-- GPUB — migration 01: host agents, telemetry, jobs, slot extension. Applied by coordinator.py initdb AFTER schema.sql.
-- Additive only; nothing dropped, nothing deleted. Safe to re-run.

-- Agent telemetry. Append-only; one row per agent report (every ~30 s per resource).
CREATE TABLE IF NOT EXISTS telemetry (
  id             bigserial PRIMARY KEY,
  ts             timestamptz NOT NULL DEFAULT now(),
  resource_id    text NOT NULL REFERENCES resources(resource_id),
  machine        text NOT NULL REFERENCES machines(machine),
  gpu            jsonb,                 -- {util_pct, temp_c, mem_used_gb, mem_total_gb, power_w, apps:[{pid,name,mem_mb}]}  (N/A -> null)
  ollama         jsonb,                 -- {loaded:[{name, size_gb}], reachable}
  agent          jsonb,                 -- {version, pid, uptime_s, online_since, from_cache_jobs, skew_s}
  busy           boolean NOT NULL DEFAULT false   -- any compute app or loaded model observed in this sample
);
CREATE INDEX IF NOT EXISTS telemetry_res_ts_idx ON telemetry (resource_id, ts DESC);

-- Jobs: the host agent executes booking.job at slot start.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS from_cache boolean NOT NULL DEFAULT false;   -- started from the agent's local cache while the coordinator was unreachable
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS agent_machine text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS note text;
CREATE INDEX IF NOT EXISTS jobs_booking_idx ON jobs (booking_id);

-- Slot extension: the agent asks for +N min while its job is still running;
-- the DB decides (EXCLUDE constraint + capacity trigger fire on UPDATE OF slot).
ALTER TABLE bookings ADD COLUMN IF NOT EXISTS extended_min integer NOT NULL DEFAULT 0 CHECK (extended_min >= 0);

-- No DELETE on the new table either.
DO $$ BEGIN
  EXECUTE 'DROP TRIGGER IF EXISTS no_delete_trg ON telemetry';
  EXECUTE 'CREATE TRIGGER no_delete_trg BEFORE DELETE ON telemetry FOR EACH ROW EXECUTE FUNCTION gpub_no_delete()';
END $$;
