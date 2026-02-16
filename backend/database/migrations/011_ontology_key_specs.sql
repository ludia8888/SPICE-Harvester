-- Ontology key_spec registry (Postgres SSoT)
--
-- Older schema documents may discard per-property metadata (primaryKey/titleKey),
-- so we persist the ordered key spec for each ontology class and overlay it on reads.

CREATE TABLE IF NOT EXISTS ontology_key_specs (
  db_name VARCHAR(255) NOT NULL,
  branch VARCHAR(255) NOT NULL,
  class_id VARCHAR(255) NOT NULL,
  primary_key JSONB NOT NULL DEFAULT '[]'::jsonb,
  title_key JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (db_name, branch, class_id)
);

CREATE INDEX IF NOT EXISTS idx_ontology_key_specs_db_branch
  ON ontology_key_specs(db_name, branch);
