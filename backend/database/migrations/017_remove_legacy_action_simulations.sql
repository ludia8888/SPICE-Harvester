-- Remove legacy action simulation persistence tables/schemas that are no
-- longer used by Foundry-aligned apply/applyBatch runtime paths.
-- Keep older migrations for history; execute cleanup as a forward migration.

DROP TABLE IF EXISTS action_simulation_versions CASCADE;
DROP TABLE IF EXISTS action_simulations CASCADE;
DROP SCHEMA IF EXISTS spice_action_simulations CASCADE;
