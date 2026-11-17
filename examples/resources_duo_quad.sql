-- resources_duo_quad.sql — four NVIDIA GB10 / DGX Spark-class boxes as singles, two duos and one quad.
-- Apply AFTER `coordinator.py initdb` (needs sql/migration_02_pairs.sql for resources.members):
--     psql -d gpub -f examples/resources_duo_quad.sql
-- Additive and idempotent (ON CONFLICT DO NOTHING). Singles first: a group's members must exist when it is booked.
--
-- Topology assumed: the four boxes are cabled as a switchless QSFP ring; gb10-duo (spark-a + spark-b) and
-- gb10-duo-2 (spark-c + spark-d) are each a directly cabled pair, gb10-quad uses all four. GPUB does not discover
-- or change cabling: a group resource is just a list of members you declare.
--
-- THE RULE (no special code; it falls out of how groups are booked):
--   * Booking a group inserts one EXCLUSIVE booking per member, in one transaction. Any member slot that is already
--     taken (a single, a duo, a shared hold — any live booking) makes Postgres refuse the whole group: 409, no partial hold.
--   * So a quad holds all four boxes exclusive, and any live booking on any member refuses the quad.
--   * The quad never preempts: priority never bumps an existing booking. A quad waits (earliest-fit waits for the
--     busiest member) or picks a free window. Equally, while a quad is booked, singles and duos on its members are refused.
--   * The job (if any) runs on the DRIVER member = the member whose host_machine equals the group's host_machine;
--     the other members are held idle so nothing else lands on them.
--
-- capacity_gb is a CONFIGURED budget (GB10 nvidia-smi reports memory as N/A): 110 of ~121 GB unified, leaving OS headroom.
-- A group's capacity_gb is informational (groups are exclusive-only); here it is the sum of its members.

INSERT INTO machines (machine) VALUES ('spark-a'), ('spark-b'), ('spark-c'), ('spark-d') ON CONFLICT DO NOTHING;

INSERT INTO resources (resource_id, host_machine, model, capacity_gb, gpu_lock_path, notes) VALUES
  ('spark-a-gb10', 'spark-a', 'NVIDIA GB10 (DGX Spark-class)', 110, NULL, 'single; 110 GB budget of ~121 GB unified memory'),
  ('spark-b-gx10', 'spark-b', 'NVIDIA GB10 (DGX Spark-class)', 110, NULL, 'single; 110 GB budget of ~121 GB unified memory'),
  ('spark-c-gb10', 'spark-c', 'NVIDIA GB10 (DGX Spark-class)', 110, NULL, 'single; 110 GB budget of ~121 GB unified memory'),
  ('spark-d-gx10', 'spark-d', 'NVIDIA GB10 (DGX Spark-class)', 110, NULL, 'single; 110 GB budget of ~121 GB unified memory')
ON CONFLICT (resource_id) DO NOTHING;

INSERT INTO resources (resource_id, host_machine, model, capacity_gb, gpu_lock_path, members, notes) VALUES
  ('gb10-duo', 'spark-a', 'DUO: 2x GB10 over QSFP (spark-a-gb10 + spark-b-gx10)', 220, NULL,
   ARRAY['spark-a-gb10', 'spark-b-gx10'],
   'One booking holds both exclusive; a taken slot on either refuses the duo. Driver = spark-a.'),
  ('gb10-duo-2', 'spark-c', 'DUO: 2x GB10 over QSFP (spark-c-gb10 + spark-d-gx10)', 220, NULL,
   ARRAY['spark-c-gb10', 'spark-d-gx10'],
   'One booking holds both exclusive; a taken slot on either refuses the duo. Driver = spark-c.'),
  ('gb10-quad', 'spark-a', 'QUAD: 4x GB10 on a switchless QSFP ring', 440, NULL,
   ARRAY['spark-a-gb10', 'spark-b-gx10', 'spark-c-gb10', 'spark-d-gx10'],
   'Holds all four exclusive. Any live booking on any member (single or duo) refuses it; it never preempts. Driver = spark-a.')
ON CONFLICT (resource_id) DO NOTHING;
