---
orphan: true
---

# Branch `master` → `main` Data Migration Report (2026-03-03)

- Generated at: `2026-03-03T11:15:06.391320+00:00`
- Mode: `dry-run`
- Prior `--apply` run at `2026-03-03T11:14:02.047062+00:00` completed successfully.

## Summary
- Text/Varchar candidates: `0`
- Text/Varchar updated: `0`
- JSON/JSONB candidate rows: `850`
- JSON/JSONB updated rows: `0`
- lakeFS actions: `3`

## Apply Run Snapshot (completed before this dry-run)
- Text/Varchar candidates: `123101`
- Text/Varchar updated: `123078`
- JSON/JSONB candidate rows: `80780`
- JSON/JSONB updated rows: `80193`
- Conflict pruning performed on branch-unique tables:
  - `public.ontology_key_specs.branch`: `pruned_conflicts=7`
  - `public.ontology_resource_versions.branch`: `pruned_conflicts=7`
  - `public.ontology_resources.branch`: `pruned_conflicts=7`
  - `spice_datasets.datasets.branch`: `pruned_conflicts=1`
  - `spice_pipelines.pipeline_branches.branch`: `pruned_conflicts=1`

## Text/Varchar Columns
- None

## JSON/JSONB Columns
- `spice_datasets.gate_results.details`: candidates=5, updated=0
- `spice_objectify.objectify_job_outbox.payload`: candidates=181, updated=0
- `spice_objectify.objectify_jobs.report`: candidates=6, updated=0
- `spice_objectify.ontology_mapping_specs.options`: candidates=132, updated=0
- `spice_pipelines.pipeline_artifacts.inputs`: candidates=149, updated=0
- `spice_pipelines.pipeline_runs.input_lakefs_commits`: candidates=107, updated=0
- `spice_pipelines.pipeline_runs.output_json`: candidates=132, updated=0
- `spice_pipelines.pipeline_runs.sample_json`: candidates=53, updated=0
- `spice_pipelines.pipelines.last_build_output`: candidates=33, updated=0
- `spice_pipelines.pipelines.last_preview_sample`: candidates=52, updated=0

## lakeFS
- `ontology-writeback`: noop (master branch missing)
- `pipeline-artifacts`: noop (master branch missing)
- `raw-datasets`: noop (master branch missing)
