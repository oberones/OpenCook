CREATE TABLE IF NOT EXISTS oc_core_environments (
    org_name TEXT NOT NULL,
    environment_name TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_name, environment_name),
    CONSTRAINT oc_core_environments_org_fk
        FOREIGN KEY (org_name)
        REFERENCES oc_bootstrap_orgs (org_name)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS oc_core_nodes (
    org_name TEXT NOT NULL,
    node_name TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_name, node_name),
    CONSTRAINT oc_core_nodes_org_fk
        FOREIGN KEY (org_name)
        REFERENCES oc_bootstrap_orgs (org_name)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS oc_core_nodes_org_name_idx
    ON oc_core_nodes (org_name);

CREATE TABLE IF NOT EXISTS oc_core_roles (
    org_name TEXT NOT NULL,
    role_name TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_name, role_name),
    CONSTRAINT oc_core_roles_org_fk
        FOREIGN KEY (org_name)
        REFERENCES oc_bootstrap_orgs (org_name)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS oc_core_data_bags (
    org_name TEXT NOT NULL,
    bag_name TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_name, bag_name),
    CONSTRAINT oc_core_data_bags_org_fk
        FOREIGN KEY (org_name)
        REFERENCES oc_bootstrap_orgs (org_name)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS oc_core_data_bag_items (
    org_name TEXT NOT NULL,
    bag_name TEXT NOT NULL,
    item_id TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_name, bag_name, item_id),
    CONSTRAINT oc_core_data_bag_items_bag_fk
        FOREIGN KEY (org_name, bag_name)
        REFERENCES oc_core_data_bags (org_name, bag_name)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS oc_core_policy_revisions (
    org_name TEXT NOT NULL,
    policy_name TEXT NOT NULL,
    revision_id TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_name, policy_name, revision_id),
    CONSTRAINT oc_core_policy_revisions_org_fk
        FOREIGN KEY (org_name)
        REFERENCES oc_bootstrap_orgs (org_name)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS oc_core_policy_groups (
    org_name TEXT NOT NULL,
    group_name TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_name, group_name),
    CONSTRAINT oc_core_policy_groups_org_fk
        FOREIGN KEY (org_name)
        REFERENCES oc_bootstrap_orgs (org_name)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS oc_core_sandboxes (
    org_name TEXT NOT NULL,
    sandbox_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_name, sandbox_id),
    CONSTRAINT oc_core_sandboxes_org_fk
        FOREIGN KEY (org_name)
        REFERENCES oc_bootstrap_orgs (org_name)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS oc_core_sandbox_checksums (
    org_name TEXT NOT NULL,
    sandbox_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    checksum TEXT NOT NULL,
    PRIMARY KEY (org_name, sandbox_id, ordinal),
    CONSTRAINT oc_core_sandbox_checksums_sandbox_fk
        FOREIGN KEY (org_name, sandbox_id)
        REFERENCES oc_core_sandboxes (org_name, sandbox_id)
        ON DELETE CASCADE,
    CONSTRAINT oc_core_sandbox_checksums_ordinal_non_negative
        CHECK (ordinal >= 0)
);

CREATE INDEX IF NOT EXISTS oc_core_sandbox_checksums_checksum_idx
    ON oc_core_sandbox_checksums (checksum);

CREATE TABLE IF NOT EXISTS oc_core_object_acls (
    org_name TEXT NOT NULL,
    acl_key TEXT NOT NULL,
    acl_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_name, acl_key),
    CONSTRAINT oc_core_object_acls_org_fk
        FOREIGN KEY (org_name)
        REFERENCES oc_bootstrap_orgs (org_name)
        ON DELETE CASCADE
);
