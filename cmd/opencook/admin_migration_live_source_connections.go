package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type adminMigrationLiveSourcePostgresTargets struct {
	Erchef  *pgx.ConnConfig
	Bifrost *pgx.ConnConfig
}

// adminMigrationLiveSourceDerivePostgresTargets parses the cluster seed once,
// deep-clones all connection settings, and changes only the database name.
func adminMigrationLiveSourceDerivePostgresTargets(cfg adminMigrationLiveSourceConfig) (adminMigrationLiveSourcePostgresTargets, error) {
	seed, err := pgx.ParseConfig(strings.TrimSpace(cfg.PostgresDSN))
	if err != nil {
		return adminMigrationLiveSourcePostgresTargets{}, err
	}
	erchef := seed.Copy()
	erchef.Database = adminMigrationLiveSourceEffectiveErchefDatabase(cfg)
	bifrost := seed.Copy()
	bifrost.Database = adminMigrationLiveSourceEffectiveBifrostDatabase(cfg)
	return adminMigrationLiveSourcePostgresTargets{Erchef: erchef, Bifrost: bifrost}, nil
}

func adminMigrationLiveSourceEffectiveErchefDatabase(cfg adminMigrationLiveSourceConfig) string {
	if name := strings.TrimSpace(cfg.ErchefDatabase); name != "" {
		return name
	}
	return adminMigrationLiveSourceErchefDatabase
}

func adminMigrationLiveSourceEffectiveBifrostDatabase(cfg adminMigrationLiveSourceConfig) string {
	if name := strings.TrimSpace(cfg.BifrostDatabase); name != "" {
		return name
	}
	return adminMigrationLiveSourceBifrostDatabase
}

type adminMigrationLiveSourceDatabaseProbe struct {
	Dependencies  []adminMigrationDependency
	Findings      []adminMigrationFinding
	Organizations []string
	Ready         bool
}

func adminMigrationLiveSourceProbeDatabase(ctx context.Context, connConfig *pgx.ConnConfig, component string, requiredTables []string, readOrganizations bool) adminMigrationLiveSourceDatabaseProbe {
	if ctx == nil {
		ctx = context.Background()
	}
	probeCtx, cancel := context.WithTimeout(ctx, adminMigrationLiveSourcePostgresProbeTimeout)
	defer cancel()

	connectionName := "source_" + component + "_postgres"
	schemaName := "source_" + component + "_schema"
	unavailableCode := adminMigrationFindingSourceErchefUnavailable
	schemaCode := adminMigrationFindingSourceErchefSchemaUnsupported
	if component == "bifrost" {
		unavailableCode = adminMigrationFindingSourceBifrostUnavailable
		schemaCode = adminMigrationFindingSourceBifrostSchemaUnsupported
	}
	failure := func(message string, schema bool) adminMigrationLiveSourceDatabaseProbe {
		name := connectionName
		code := unavailableCode
		family := connectionName
		if schema {
			name = schemaName
			code = schemaCode
			family = schemaName
		}
		return adminMigrationLiveSourceDatabaseProbe{
			Dependencies: []adminMigrationDependency{{
				Name:       name,
				Status:     "error",
				Backend:    "postgres",
				Configured: true,
				Message:    message,
				Details: map[string]string{
					"database":           connConfig.Database,
					"read_only_required": "true",
				},
			}},
			Findings: []adminMigrationFinding{{Severity: "error", Code: code, Family: family, Message: message}},
		}
	}

	conn, err := pgx.ConnectConfig(probeCtx, connConfig)
	if err != nil {
		return failure("source "+component+" PostgreSQL could not be reached or authenticated", false)
	}
	defer adminMigrationLiveSourceCloseConnection(conn)

	tx, err := conn.BeginTx(probeCtx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return failure("source "+component+" PostgreSQL did not allow a read-only repeatable-read transaction", false)
	}
	defer adminMigrationLiveSourceRollback(tx)

	var readOnly, databaseName, databaseUser string
	if err := tx.QueryRow(probeCtx, "SHOW transaction_read_only").Scan(&readOnly); err != nil || !adminMigrationLiveSourcePostgresReadOnlyEnabled(readOnly) {
		return failure("source "+component+" PostgreSQL read-only posture could not be verified", false)
	}
	if err := tx.QueryRow(probeCtx, "SELECT current_database(), current_user").Scan(&databaseName, &databaseUser); err != nil {
		return failure("source "+component+" PostgreSQL identity could not be read", false)
	}
	missingTables, err := adminMigrationLiveSourceMissingTables(probeCtx, tx, requiredTables)
	if err != nil {
		result := failure("source "+component+" PostgreSQL schema could not be inspected by the configured read-only role", true)
		result.Dependencies = append([]adminMigrationDependency{adminMigrationLiveSourceConnectionDependency(component, databaseName, databaseUser, "read-only repeatable-read connection passed")}, result.Dependencies...)
		return result
	}
	if len(missingTables) > 0 {
		result := failure("source "+component+" PostgreSQL schema is missing required Chef Server tables", true)
		result.Dependencies = append([]adminMigrationDependency{adminMigrationLiveSourceConnectionDependency(component, databaseName, databaseUser, "read-only repeatable-read connection passed")}, result.Dependencies...)
		result.Dependencies[len(result.Dependencies)-1].Details["missing_tables"] = adminMigrationLiveSourceBoundedList(missingTables, 8)
		result.Dependencies[len(result.Dependencies)-1].Details["missing_count"] = fmt.Sprintf("%d", len(missingTables))
		result.Dependencies[len(result.Dependencies)-1].Details["required_tables"] = fmt.Sprintf("%d", len(requiredTables))
		return result
	}

	organizations := []string(nil)
	if readOrganizations {
		organizations, err = adminMigrationLiveSourceReadOrganizations(probeCtx, tx)
		if err != nil {
			result := failure("source erchef organizations could not be enumerated by the configured read-only role", true)
			result.Dependencies = append([]adminMigrationDependency{adminMigrationLiveSourceConnectionDependency(component, databaseName, databaseUser, "read-only repeatable-read connection passed")}, result.Dependencies...)
			return result
		}
	}
	return adminMigrationLiveSourceDatabaseProbe{
		Dependencies: []adminMigrationDependency{
			adminMigrationLiveSourceConnectionDependency(component, databaseName, databaseUser, "read-only repeatable-read connection passed"),
			{
				Name:       schemaName,
				Status:     "ok",
				Backend:    "postgres",
				Configured: true,
				Message:    "source " + component + " PostgreSQL schema exposes required tables",
				Details: map[string]string{
					"database":        adminMigrationLiveSourceSafeDetail(databaseName),
					"required_tables": fmt.Sprintf("%d", len(requiredTables)),
				},
			},
		},
		Organizations: organizations,
		Ready:         true,
	}
}

func adminMigrationLiveSourceConnectionDependency(component, databaseName, databaseUser, message string) adminMigrationDependency {
	return adminMigrationDependency{
		Name:       "source_" + component + "_postgres",
		Status:     "ok",
		Backend:    "postgres",
		Configured: true,
		Message:    "source " + component + " PostgreSQL " + message,
		Details: map[string]string{
			"database":  adminMigrationLiveSourceSafeDetail(databaseName),
			"user":      adminMigrationLiveSourceSafeDetail(databaseUser),
			"read_only": "true",
			"isolation": "repeatable_read",
		},
	}
}

func adminMigrationLiveSourceMissingTables(ctx context.Context, tx pgx.Tx, requiredTables []string) ([]string, error) {
	missing := make([]string, 0)
	for _, table := range requiredTables {
		var resolved string
		if err := tx.QueryRow(ctx, "SELECT COALESCE(to_regclass($1)::text, '')", table).Scan(&resolved); err != nil {
			return nil, err
		}
		if strings.TrimSpace(resolved) == "" {
			missing = append(missing, table)
		}
	}
	return missing, nil
}

func adminMigrationLiveSourceRollback(tx pgx.Tx) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(cleanupCtx)
}

func adminMigrationLiveSourceCloseConnection(conn *pgx.Conn) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = conn.Close(cleanupCtx)
}

func adminMigrationLiveSourceCrossDatabaseConsistencyFinding() adminMigrationFinding {
	return adminMigrationFinding{
		Severity: "warning",
		Code:     adminMigrationFindingSourceCrossDatabaseConsistency,
		Family:   "live_source",
		Message:  "Erchef and Bifrost use independent repeatable-read snapshots; active-source rehearsal is advisory and final cutover requires external source-freeze evidence",
	}
}

func adminMigrationLiveSourceRequiredErchefTables() []string {
	return []string{
		"users", "keys", "orgs", "org_user_associations", "clients", "groups", "containers",
		"nodes", "environments", "roles", "data_bags", "data_bag_items", "policies", "policy_revisions", "policy_groups",
		"policy_revisions_policy_groups_association", "checksums", "sandboxed_checksums", "cookbooks", "cookbook_versions",
		"cookbook_version_checksums", "cookbook_artifacts", "cookbook_artifact_versions", "cookbook_artifact_version_checksums",
	}
}

func adminMigrationLiveSourceRequiredBifrostTables() []string {
	return []string{
		"auth_container", "auth_actor", "auth_group", "auth_object",
		"object_acl_group", "object_acl_actor", "actor_acl_group", "actor_acl_actor",
		"group_acl_group", "group_acl_actor", "container_acl_group", "container_acl_actor",
		"group_group_relations", "group_actor_relations",
	}
}
