-- GPUB (GPU Booker) ledger — schema.sql
-- Database: gpub (Postgres 16 tested; needs btree_gist). Applied by: gpub/coordinator.py initdb, then the migration_*.sql files in order.
-- Doctrine: all times timestamptz (UTC on the wire); the DATABASE prevents conflicts, not the API.
--   * exclusive-vs-exclusive overlap  -> EXCLUDE constraint (btree_gist)         [acceptance test 1]
--   * exclusive-vs-shared / shared sum -> BEFORE trigger, exact per-boundary sum  [acceptance test 2]
--   * every state change               -> audit_log row via AFTER trigger
-- Never DELETE a booking: states released/expired/preempted/cancelled are the tombstones (enforced by trigger below).

CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE IF NOT EXISTS resources (
  resource_id    text PRIMARY KEY,                 -- 'rtxbox-5090', 'spark-a-gb10'; add a GPU = add a row
  host_machine   text NOT NULL,
  model          text,
  capacity_gb    numeric NOT NULL CHECK (capacity_gb > 0),   -- CONFIGURED, never read live (GB10 reports N/A)
  gpu_lock_path  text,                             -- optional external lockfile acquired on the holder's behalf; NULL = none
  enabled        boolean NOT NULL DEFAULT true,
  notes          text,
  created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS machines (
  machine        text PRIMARY KEY,                 -- 'headnode','rtxbox','spark-a','workstation', ...
  tailnet_name   text,
  tailnet_ip     inet UNIQUE,                      -- private LAN/VPN IP; the caller's source IP must match this (identity, test T10)
  os             text,
  token_hash     text,                             -- sha256(token); the token itself is printed once at registration
  enabled        boolean NOT NULL DEFAULT true,
  registered_at  timestamptz NOT NULL DEFAULT now(),
  last_seen      timestamptz
);

CREATE TABLE IF NOT EXISTS bookings (
  id             bigserial PRIMARY KEY,
  resource_id    text NOT NULL REFERENCES resources(resource_id),
  owner_machine  text NOT NULL REFERENCES machines(machine),
  mode           text NOT NULL CHECK (mode IN ('exclusive','shared')),
  vram_budget_gb numeric CHECK (vram_budget_gb > 0),
  priority       text NOT NULL DEFAULT 'interactive'
                 CHECK (priority IN ('critical','sweep','interactive','background')),
  state          text NOT NULL DEFAULT 'booked'
                 CHECK (state IN ('template','booked','active','released','expired','preempted','cancelled')),
  end_reason     text,                             -- 'holder_release','slot_end','no_show','heartbeat_lost','cancelled', ...
  purpose        text NOT NULL DEFAULT '',
  slot           tstzrange NOT NULL
                 CHECK (NOT isempty(slot) AND lower_inc(slot) AND NOT upper_inc(slot)
                        AND lower(slot) IS NOT NULL AND upper(slot) IS NOT NULL),
  tz             text NOT NULL DEFAULT 'UTC',      -- owner's IANA zone; RRULE expands in THIS zone (DST-safe)
  rrule          text,                             -- RFC 5545; present only on 'template' rows
  parent_id      bigint REFERENCES bookings(id),   -- expanded occurrence -> its template
  job            jsonb,                            -- {cmd, cwd, env, on_slot_end, extend, release_on_exit}; executed by the host agent
  enforce        text NOT NULL DEFAULT 'soft' CHECK (enforce IN ('soft','hard')),
  grace_min      integer NOT NULL DEFAULT 5 CHECK (grace_min >= 0),   -- 0 = no heartbeat required
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  ended_at       timestamptz,
  CONSTRAINT shared_has_budget CHECK ((mode = 'shared') = (vram_budget_gb IS NOT NULL)),
  -- a template keeps its rrule when cancelled (tombstone), so the implication runs one way only
  CONSTRAINT template_has_rrule CHECK (state <> 'template' OR (rrule IS NOT NULL AND parent_id IS NULL)),
  CONSTRAINT occurrence_has_no_rrule CHECK (parent_id IS NULL OR rrule IS NULL),
  -- THE constraint. Two live exclusive bookings can never overlap on one resource, whoever inserts them.
  CONSTRAINT no_exclusive_overlap EXCLUDE USING gist (resource_id WITH =, slot WITH &&)
    WHERE (mode = 'exclusive' AND state IN ('booked','active'))
);
CREATE INDEX IF NOT EXISTS bookings_live_idx ON bookings USING gist (resource_id, slot)
  WHERE state IN ('booked','active');
CREATE INDEX IF NOT EXISTS bookings_parent_idx ON bookings (parent_id);

CREATE TABLE IF NOT EXISTS leases (
  id             bigserial PRIMARY KEY,
  booking_id     bigint NOT NULL UNIQUE REFERENCES bookings(id),
  holder_machine text NOT NULL REFERENCES machines(machine),
  started_at     timestamptz NOT NULL DEFAULT now(),
  last_heartbeat timestamptz NOT NULL DEFAULT now(),
  heartbeats     integer NOT NULL DEFAULT 0,
  state          text NOT NULL DEFAULT 'active' CHECK (state IN ('active','released','expired')),
  gpu_lock_held  boolean NOT NULL DEFAULT false,
  ended_at       timestamptz
);

CREATE TABLE IF NOT EXISTS jobs (                  -- filled by host agents (migration_01 adds columns)
  id             bigserial PRIMARY KEY,
  booking_id     bigint NOT NULL REFERENCES bookings(id),
  host_machine   text NOT NULL,
  cmd            text NOT NULL,
  cwd            text,
  run_user       text,
  state          text NOT NULL DEFAULT 'queued' CHECK (state IN ('queued','running','exited','failed','killed')),
  pid            integer,
  log_path       text,
  exit_code      integer,
  started_at     timestamptz,
  ended_at       timestamptz
);

CREATE SEQUENCE IF NOT EXISTS signal_seq;
CREATE TABLE IF NOT EXISTS signals (
  id             bigserial PRIMARY KEY,
  signal_id      text NOT NULL UNIQUE,             -- DS-YYYY-NNNNNN (signal id)
  ts             timestamptz NOT NULL DEFAULT now(),
  from_machine   text NOT NULL,                    -- machine or 'coordinator'
  to_machines    text[] NOT NULL DEFAULT '{*}',
  type           text NOT NULL,
  severity       text NOT NULL CHECK (severity IN ('green','yellow','red')),
  resource_id    text,
  booking_id     bigint,
  message        text NOT NULL,
  ack_required   boolean NOT NULL DEFAULT false,
  mirrored       boolean NOT NULL DEFAULT false    -- reserved for mirroring signals to an external channel (unused)
);
CREATE INDEX IF NOT EXISTS signals_ts_idx ON signals (ts DESC);

CREATE TABLE IF NOT EXISTS signal_acks (
  id             bigserial PRIMARY KEY,
  signal_id      text NOT NULL REFERENCES signals(signal_id),
  acked_by       text NOT NULL,
  acked_at       timestamptz NOT NULL DEFAULT now(),
  note           text
);

CREATE TABLE IF NOT EXISTS audit_log (
  id             bigserial PRIMARY KEY,
  ts             timestamptz NOT NULL DEFAULT now(),
  actor          text NOT NULL,                    -- machine name, 'coordinator', or 'sql:<role>'
  action         text NOT NULL,
  booking_id     bigint,
  before         jsonb,
  after          jsonb
);

-- ---------------------------------------------------------------------------------------------
-- Capacity trigger: the half of conflict prevention the EXCLUDE constraint cannot express.
--   * an exclusive booking may not overlap any live shared booking
--   * a shared booking may not overlap any live exclusive booking
--   * the sum of live shared budgets at ANY instant within the new slot may not exceed capacity
-- Serialised per resource with a row lock, so two concurrent shared inserts cannot both squeeze in.
-- ---------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION bookings_capacity_check() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  cap      numeric;
  en       boolean;
  n_other  integer;
  peak     numeric;
BEGIN
  IF NEW.state NOT IN ('booked','active') THEN
    RETURN NEW;                                    -- tombstones and templates never contend
  END IF;
  SELECT capacity_gb, enabled INTO cap, en FROM resources WHERE resource_id = NEW.resource_id FOR UPDATE;
  IF cap IS NULL THEN
    RAISE EXCEPTION 'gpub: unknown resource %', NEW.resource_id USING ERRCODE = 'foreign_key_violation';
  END IF;
  IF NOT en THEN
    RAISE EXCEPTION 'gpub: resource % is disabled', NEW.resource_id USING ERRCODE = 'check_violation';
  END IF;

  IF NEW.mode = 'exclusive' THEN
    SELECT count(*) INTO n_other FROM bookings b
     WHERE b.resource_id = NEW.resource_id AND b.id <> NEW.id
       AND b.mode = 'shared' AND b.state IN ('booked','active') AND b.slot && NEW.slot;
    IF n_other > 0 THEN
      RAISE EXCEPTION 'gpub: exclusive booking overlaps % live shared booking(s) on %', n_other, NEW.resource_id
        USING ERRCODE = 'exclusion_violation';
    END IF;
  ELSE
    SELECT count(*) INTO n_other FROM bookings b
     WHERE b.resource_id = NEW.resource_id AND b.id <> NEW.id
       AND b.mode = 'exclusive' AND b.state IN ('booked','active') AND b.slot && NEW.slot;
    IF n_other > 0 THEN
      RAISE EXCEPTION 'gpub: shared booking overlaps % live exclusive booking(s) on %', n_other, NEW.resource_id
        USING ERRCODE = 'exclusion_violation';
    END IF;
    -- exact peak: evaluate the sum at every lower boundary that falls inside the new slot
    SELECT coalesce(max(s), 0) INTO peak FROM (
      SELECT (SELECT coalesce(sum(b2.vram_budget_gb), 0) FROM bookings b2
               WHERE b2.resource_id = NEW.resource_id AND b2.id <> NEW.id
                 AND b2.mode = 'shared' AND b2.state IN ('booked','active') AND b2.slot @> p.pt) AS s
        FROM (SELECT lower(NEW.slot) AS pt
              UNION
              SELECT lower(b.slot) FROM bookings b
               WHERE b.resource_id = NEW.resource_id AND b.id <> NEW.id
                 AND b.mode = 'shared' AND b.state IN ('booked','active') AND b.slot && NEW.slot) p
       WHERE NEW.slot @> p.pt
    ) q;
    IF peak + NEW.vram_budget_gb > cap THEN
      RAISE EXCEPTION 'gpub: shared budget % GB would push % to % GB over its % GB capacity',
        NEW.vram_budget_gb, NEW.resource_id, peak + NEW.vram_budget_gb, cap
        USING ERRCODE = 'exclusion_violation';
    END IF;
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bookings_capacity_trg ON bookings;
CREATE TRIGGER bookings_capacity_trg
  BEFORE INSERT OR UPDATE OF slot, mode, vram_budget_gb, state, resource_id ON bookings
  FOR EACH ROW EXECUTE FUNCTION bookings_capacity_check();

-- Audit: every insert and every state/slot change, whoever did it (API sets gpub.actor; raw SQL shows the role).
CREATE OR REPLACE FUNCTION bookings_audit() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  actor text := coalesce(nullif(current_setting('gpub.actor', true), ''), 'sql:' || current_user);
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO audit_log (actor, action, booking_id, before, after)
      VALUES (actor, 'booking_created', NEW.id, NULL, to_jsonb(NEW));
  ELSIF NEW.state <> OLD.state OR NEW.slot <> OLD.slot THEN
    INSERT INTO audit_log (actor, action, booking_id, before, after)
      VALUES (actor, 'booking_' || OLD.state || '_to_' || NEW.state, NEW.id, to_jsonb(OLD), to_jsonb(NEW));
  END IF;
  RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS bookings_audit_trg ON bookings;
CREATE TRIGGER bookings_audit_trg AFTER INSERT OR UPDATE ON bookings
  FOR EACH ROW EXECUTE FUNCTION bookings_audit();

-- Nothing is deleted from the ledger. Enforced, not requested.
CREATE OR REPLACE FUNCTION gpub_no_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'gpub: DELETE is forbidden on % (append-only ledger). Change state instead.', TG_TABLE_NAME;
END $$;
DO $$ DECLARE t text; BEGIN
  FOREACH t IN ARRAY ARRAY['bookings','leases','signals','audit_log','jobs'] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS no_delete_trg ON %I', t);
    EXECUTE format('CREATE TRIGGER no_delete_trg BEFORE DELETE ON %I FOR EACH ROW EXECUTE FUNCTION gpub_no_delete()', t);
  END LOOP;
END $$;
