-- data_warehouse/migrations/V003__add_config_audit.sql
-- NOTE: This trigger only audits UPDATES. The initial seed in V001 is NOT 
-- recorded in the audit log unless it is updated after this trigger is created.
CREATE TABLE IF NOT EXISTS app_config_audit (
    id          SERIAL PRIMARY KEY,
    key         VARCHAR(64) NOT NULL,
    old_value   VARCHAR(256),
    new_value   VARCHAR(256) NOT NULL,
    changed_at  TIMESTAMPTZ DEFAULT NOW(),
    changed_by  VARCHAR(64) DEFAULT 'dashboard'
);

CREATE OR REPLACE FUNCTION audit_app_config_change()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.value IS DISTINCT FROM NEW.value THEN
        INSERT INTO app_config_audit (key, old_value, new_value)
        VALUES (NEW.key, OLD.value, NEW.value);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_app_config_audit
    AFTER UPDATE ON app_config
    FOR EACH ROW EXECUTE FUNCTION audit_app_config_change();
