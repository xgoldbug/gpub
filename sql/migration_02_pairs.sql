-- GPUB — migration 02: group ("pair") resources. Applied AFTER schema.sql + migration_01_agents.sql. Additive only.
-- A group resource (2-box duo, 4-box quad, ...) lists its members; booking it
-- creates one exclusive member booking per member in the same transaction, so a slot that is taken on any
-- member refuses the whole group (the EXCLUDE constraint + capacity trigger on the members decide; no new logic).
ALTER TABLE resources ADD COLUMN IF NOT EXISTS members text[];            -- NULL = an ordinary GPU
ALTER TABLE bookings  ADD COLUMN IF NOT EXISTS pair_id bigint REFERENCES bookings(id);   -- member booking -> its pair booking
CREATE INDEX IF NOT EXISTS bookings_pair_idx ON bookings (pair_id);
