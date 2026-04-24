CREATE TABLE IF NOT EXISTS oc_bootstrap_users (
    username TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    email TEXT NOT NULL DEFAULT '',
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oc_bootstrap_user_acls (
    username TEXT PRIMARY KEY,
    acl_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT oc_bootstrap_user_acls_user_fk
        FOREIGN KEY (username)
        REFERENCES oc_bootstrap_users (username)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS oc_bootstrap_user_keys (
    username TEXT NOT NULL,
    key_name TEXT NOT NULL,
    uri TEXT NOT NULL,
    public_key_pem TEXT NOT NULL,
    expiration_date TEXT NOT NULL,
    expires_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (username, key_name),
    CONSTRAINT oc_bootstrap_user_keys_user_fk
        FOREIGN KEY (username)
        REFERENCES oc_bootstrap_users (username)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS oc_bootstrap_orgs (
    org_name TEXT PRIMARY KEY,
    full_name TEXT NOT NULL,
    org_type TEXT NOT NULL,
    guid TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oc_bootstrap_clients (
    org_name TEXT NOT NULL,
    client_name TEXT NOT NULL,
    name TEXT NOT NULL,
    validator BOOLEAN NOT NULL DEFAULT FALSE,
    admin BOOLEAN NOT NULL DEFAULT FALSE,
    public_key_pem TEXT NOT NULL DEFAULT '',
    uri TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_name, client_name),
    CONSTRAINT oc_bootstrap_clients_org_fk
        FOREIGN KEY (org_name)
        REFERENCES oc_bootstrap_orgs (org_name)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS oc_bootstrap_client_keys (
    org_name TEXT NOT NULL,
    client_name TEXT NOT NULL,
    key_name TEXT NOT NULL,
    uri TEXT NOT NULL,
    public_key_pem TEXT NOT NULL,
    expiration_date TEXT NOT NULL,
    expires_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_name, client_name, key_name),
    CONSTRAINT oc_bootstrap_client_keys_client_fk
        FOREIGN KEY (org_name, client_name)
        REFERENCES oc_bootstrap_clients (org_name, client_name)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS oc_bootstrap_groups (
    org_name TEXT NOT NULL,
    group_name TEXT NOT NULL,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_name, group_name),
    CONSTRAINT oc_bootstrap_groups_org_fk
        FOREIGN KEY (org_name)
        REFERENCES oc_bootstrap_orgs (org_name)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS oc_bootstrap_group_memberships (
    org_name TEXT NOT NULL,
    group_name TEXT NOT NULL,
    member_type TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    member_name TEXT NOT NULL,
    PRIMARY KEY (org_name, group_name, member_type, ordinal),
    CONSTRAINT oc_bootstrap_group_memberships_group_fk
        FOREIGN KEY (org_name, group_name)
        REFERENCES oc_bootstrap_groups (org_name, group_name)
        ON DELETE CASCADE,
    CONSTRAINT oc_bootstrap_group_memberships_type_check
        CHECK (member_type IN ('actor', 'user', 'client', 'group')),
    CONSTRAINT oc_bootstrap_group_memberships_ordinal_non_negative
        CHECK (ordinal >= 0)
);

CREATE TABLE IF NOT EXISTS oc_bootstrap_containers (
    org_name TEXT NOT NULL,
    container_name TEXT NOT NULL,
    name TEXT NOT NULL,
    container_path TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_name, container_name),
    CONSTRAINT oc_bootstrap_containers_org_fk
        FOREIGN KEY (org_name)
        REFERENCES oc_bootstrap_orgs (org_name)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS oc_bootstrap_org_acls (
    org_name TEXT NOT NULL,
    acl_key TEXT NOT NULL,
    acl_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_name, acl_key),
    CONSTRAINT oc_bootstrap_org_acls_org_fk
        FOREIGN KEY (org_name)
        REFERENCES oc_bootstrap_orgs (org_name)
        ON DELETE CASCADE
);
