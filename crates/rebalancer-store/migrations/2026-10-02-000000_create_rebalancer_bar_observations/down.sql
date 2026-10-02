-- Reverses 2026-10-02-000000_create_rebalancer_bar_observations. The tables are append-only by trigger; dropping
-- them is the only way their rows go away, and it discards the latency / revision evidence the paper cycle is gated
-- on (R25 precondition (6)), so this is an owner decision, never part of a routine rollback.
DROP TABLE IF EXISTS rebalancer_bar_revisions;
DROP TABLE IF EXISTS rebalancer_bar_first_seen;
