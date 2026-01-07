-- Database access registry (owner/share roles)

CREATE TABLE IF NOT EXISTS database_access (
    db_name TEXT NOT NULL,
    principal_type TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    principal_name TEXT,
    role TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (db_name, principal_type, principal_id)
);

DO $$
BEGIN
    ALTER TABLE database_access
        ADD CONSTRAINT database_access_role_check
        CHECK (role IN ('Owner', 'Editor', 'Viewer', 'DomainModeler', 'DataEngineer', 'Security'));
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_database_access_db_name
    ON database_access(db_name);
CREATE INDEX IF NOT EXISTS idx_database_access_principal
    ON database_access(principal_type, principal_id);
