-- Normalize file import config key to file_import_config only.
-- 1) Backfill file_import_config from legacy table_import_config when missing.
-- 2) Drop legacy table_import_config key for *_file_import rows.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'spice_connectors'
          AND table_name = 'connector_sources'
    ) THEN
        UPDATE spice_connectors.connector_sources
        SET config_json = jsonb_set(
            config_json,
            '{file_import_config}',
            config_json->'table_import_config',
            true
        )
        WHERE source_type LIKE '%\_file_import' ESCAPE '\'
          AND (config_json ? 'table_import_config')
          AND (
            NOT (config_json ? 'file_import_config')
            OR jsonb_typeof(config_json->'file_import_config') <> 'object'
            OR config_json->'file_import_config' = '{}'::jsonb
          );

        UPDATE spice_connectors.connector_sources
        SET config_json = config_json - 'table_import_config'
        WHERE source_type LIKE '%\_file_import' ESCAPE '\'
          AND (config_json ? 'table_import_config');
    END IF;
END $$;
