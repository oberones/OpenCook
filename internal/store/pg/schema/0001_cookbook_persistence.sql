CREATE TABLE IF NOT EXISTS oc_cookbook_orgs (
    org_name TEXT PRIMARY KEY,
    full_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oc_cookbook_versions (
    org_name TEXT NOT NULL,
    cookbook_name TEXT NOT NULL,
    version TEXT NOT NULL,
    full_name TEXT NOT NULL,
    json_class TEXT NOT NULL,
    chef_type TEXT NOT NULL,
    frozen BOOLEAN NOT NULL DEFAULT FALSE,
    metadata_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_name, cookbook_name, version)
);

CREATE INDEX IF NOT EXISTS oc_cookbook_versions_org_name_idx
    ON oc_cookbook_versions (org_name, cookbook_name);

CREATE TABLE IF NOT EXISTS oc_cookbook_version_files (
    org_name TEXT NOT NULL,
    cookbook_name TEXT NOT NULL,
    version TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    file_name TEXT NOT NULL,
    file_path TEXT NOT NULL,
    checksum TEXT NOT NULL,
    specificity TEXT NOT NULL,
    PRIMARY KEY (org_name, cookbook_name, version, ordinal),
    CONSTRAINT oc_cookbook_version_files_parent_fk
        FOREIGN KEY (org_name, cookbook_name, version)
        REFERENCES oc_cookbook_versions (org_name, cookbook_name, version)
        ON DELETE CASCADE,
    CONSTRAINT oc_cookbook_version_files_ordinal_non_negative
        CHECK (ordinal >= 0)
);

CREATE INDEX IF NOT EXISTS oc_cookbook_version_files_checksum_idx
    ON oc_cookbook_version_files (checksum);

CREATE TABLE IF NOT EXISTS oc_cookbook_artifacts (
    org_name TEXT NOT NULL,
    name TEXT NOT NULL,
    identifier TEXT NOT NULL,
    version TEXT NOT NULL,
    chef_type TEXT NOT NULL,
    frozen BOOLEAN NOT NULL DEFAULT FALSE,
    metadata_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (org_name, name, identifier)
);

CREATE INDEX IF NOT EXISTS oc_cookbook_artifacts_org_name_idx
    ON oc_cookbook_artifacts (org_name, name);

CREATE TABLE IF NOT EXISTS oc_cookbook_artifact_files (
    org_name TEXT NOT NULL,
    name TEXT NOT NULL,
    identifier TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    file_name TEXT NOT NULL,
    file_path TEXT NOT NULL,
    checksum TEXT NOT NULL,
    specificity TEXT NOT NULL,
    PRIMARY KEY (org_name, name, identifier, ordinal),
    CONSTRAINT oc_cookbook_artifact_files_parent_fk
        FOREIGN KEY (org_name, name, identifier)
        REFERENCES oc_cookbook_artifacts (org_name, name, identifier)
        ON DELETE CASCADE,
    CONSTRAINT oc_cookbook_artifact_files_ordinal_non_negative
        CHECK (ordinal >= 0)
);

CREATE INDEX IF NOT EXISTS oc_cookbook_artifact_files_checksum_idx
    ON oc_cookbook_artifact_files (checksum);
