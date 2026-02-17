-- Remove legacy pull-request governance tables that are no longer part of the
-- Foundry-aligned public/runtime surface.
-- Keep older migrations for history; execute cleanup as a forward migration.

DROP TABLE IF EXISTS pull_request_reviews CASCADE;
DROP TABLE IF EXISTS pull_request_comments CASCADE;
DROP TABLE IF EXISTS pull_requests CASCADE;
