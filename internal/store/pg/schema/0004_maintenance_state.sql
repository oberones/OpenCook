CREATE TABLE IF NOT EXISTS oc_maintenance_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    mode TEXT NOT NULL,
    reason TEXT NOT NULL,
    actor TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT oc_maintenance_state_singleton_check
        CHECK (singleton),
    CONSTRAINT oc_maintenance_state_expiration_check
        CHECK (expires_at IS NULL OR expires_at > created_at)
);
