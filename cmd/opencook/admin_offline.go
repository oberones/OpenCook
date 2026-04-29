package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/store/pg"
)

type adminOfflineStore interface {
	LoadBootstrapCore() (bootstrap.BootstrapCoreState, error)
	SaveBootstrapCore(bootstrap.BootstrapCoreState) error
	LoadCoreObjects() (bootstrap.CoreObjectState, error)
	SaveCoreObjects(bootstrap.CoreObjectState) error
}

type postgresAdminOfflineStore struct {
	bootstrap bootstrap.BootstrapCoreStore
	objects   bootstrap.CoreObjectStore
	cookbooks bootstrap.CookbookStore
}

func (s postgresAdminOfflineStore) LoadBootstrapCore() (bootstrap.BootstrapCoreState, error) {
	return s.bootstrap.LoadBootstrapCore()
}

func (s postgresAdminOfflineStore) SaveBootstrapCore(state bootstrap.BootstrapCoreState) error {
	return s.bootstrap.SaveBootstrapCore(state)
}

func (s postgresAdminOfflineStore) LoadCoreObjects() (bootstrap.CoreObjectState, error) {
	return s.objects.LoadCoreObjects()
}

func (s postgresAdminOfflineStore) SaveCoreObjects(state bootstrap.CoreObjectState) error {
	return s.objects.SaveCoreObjects(state)
}

// LoadCookbookInventory reads cookbook and artifact metadata through the same
// activated cookbook store used by the app, without touching blob contents.
func (s postgresAdminOfflineStore) LoadCookbookInventory(orgNames []string) (map[string]adminMigrationCookbookInventory, error) {
	out := make(map[string]adminMigrationCookbookInventory, len(orgNames))
	if s.cookbooks == nil {
		return out, nil
	}
	for _, orgName := range orgNames {
		inventory := adminMigrationCookbookInventory{}
		if versions, ok := s.cookbooks.ListCookbookVersions(orgName); ok {
			for cookbookName, refs := range versions {
				inventory.Versions += len(refs)
				models, _, _ := s.cookbooks.ListCookbookVersionModelsByName(orgName, cookbookName)
				for _, model := range models {
					checksums := adminMigrationCookbookFileChecksums(model.AllFiles)
					inventory.ChecksumReferences += len(checksums)
					inventory.Checksums = append(inventory.Checksums, checksums...)
				}
			}
		}
		if artifacts, ok := s.cookbooks.ListCookbookArtifacts(orgName); ok {
			for _, items := range artifacts {
				inventory.Artifacts += len(items)
				for _, item := range items {
					checksums := adminMigrationCookbookFileChecksums(item.AllFiles)
					inventory.ChecksumReferences += len(checksums)
					inventory.Checksums = append(inventory.Checksums, checksums...)
				}
			}
		}
		out[orgName] = inventory
	}
	return out, nil
}

// LoadCookbookExport returns logical cookbook metadata for backup bundles
// without reading provider-backed blob bytes.
func (s postgresAdminOfflineStore) LoadCookbookExport(orgNames []string) (adminMigrationCookbookExport, error) {
	out := adminMigrationCookbookExport{Orgs: make(map[string]adminMigrationCookbookOrgExport, len(orgNames))}
	if s.cookbooks == nil {
		return out, nil
	}
	for _, orgName := range orgNames {
		orgExport := adminMigrationCookbookOrgExport{}
		if versions, ok := s.cookbooks.ListCookbookVersions(orgName); ok {
			for cookbookName := range versions {
				models, _, _ := s.cookbooks.ListCookbookVersionModelsByName(orgName, cookbookName)
				orgExport.Versions = append(orgExport.Versions, models...)
			}
		}
		if artifacts, ok := s.cookbooks.ListCookbookArtifacts(orgName); ok {
			for _, items := range artifacts {
				orgExport.Artifacts = append(orgExport.Artifacts, items...)
			}
		}
		out.Orgs[orgName] = adminMigrationSortedCookbookExportOrg(orgExport)
	}
	return out, nil
}

// RestoreCookbookExport imports logical cookbook metadata after blob content
// has been restored or verified by migration restore apply.
func (s postgresAdminOfflineStore) RestoreCookbookExport(state bootstrap.BootstrapCoreState, export adminMigrationCookbookExport) error {
	if s.cookbooks == nil {
		if adminMigrationCookbookExportCount(export) == 0 {
			return nil
		}
		return fmt.Errorf("cookbook store is not available")
	}
	registrar, _ := s.cookbooks.(interface{ EnsureOrganization(bootstrap.Organization) })
	for _, orgName := range adminMigrationSortedMapKeys(export.Orgs) {
		if registrar != nil {
			org := state.Orgs[orgName].Organization
			if strings.TrimSpace(org.Name) == "" {
				org = bootstrap.Organization{Name: orgName, FullName: orgName}
			}
			registrar.EnsureOrganization(org)
		}
		orgExport := export.Orgs[orgName]
		for _, version := range orgExport.Versions {
			if _, _, _, err := s.cookbooks.UpsertCookbookVersionWithReleasedChecksums(orgName, version, true); err != nil {
				return fmt.Errorf("restore cookbook version %s/%s/%s: %w", orgName, version.CookbookName, version.Version, err)
			}
		}
		for _, artifact := range orgExport.Artifacts {
			if _, err := s.cookbooks.CreateCookbookArtifact(orgName, artifact); err != nil {
				return fmt.Errorf("restore cookbook artifact %s/%s/%s: %w", orgName, artifact.Name, artifact.Identifier, err)
			}
		}
	}
	return nil
}

// newPostgresAdminOfflineStore activates all PostgreSQL-backed persistence
// families needed by offline admin and migration tooling.
func newPostgresAdminOfflineStore(ctx context.Context, dsn string) (adminOfflineStore, func() error, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return nil, nil, fmt.Errorf("postgres DSN is required for offline admin commands")
	}
	store := pg.New(dsn)
	if err := store.ActivateCookbookPersistence(ctx); err != nil {
		return nil, nil, err
	}
	return postgresAdminOfflineStore{
		bootstrap: store.BootstrapCore(),
		objects:   store.CoreObjects(),
		cookbooks: store.CookbookStore(),
	}, store.Close, nil
}

func adminCommandIsOffline(args []string) bool {
	if len(args) == 0 {
		return false
	}
	switch args[0] {
	case "server-admins":
		return true
	case "orgs", "organizations":
		return len(args) > 1 && (args[1] == "add-user" || args[1] == "remove-user")
	case "groups":
		return len(args) > 1 && (args[1] == "add-actor" || args[1] == "remove-actor")
	case "acls":
		return len(args) > 1 && args[1] == "repair-defaults"
	default:
		return false
	}
}

func (c *command) runAdminOfflineCommand(ctx context.Context, args []string) int {
	switch args[0] {
	case "server-admins":
		return c.runAdminServerAdminsOffline(ctx, args[1:])
	case "orgs", "organizations":
		return c.runAdminOrgMembershipOffline(ctx, args[1:])
	case "groups":
		return c.runAdminGroupMembershipOffline(ctx, args[1:])
	case "acls":
		return c.runAdminACLRepairOffline(ctx, args[1:])
	default:
		return c.adminUsageError("unknown offline admin command %q\n\n", args[0])
	}
}

func (c *command) runAdminOrgMembershipOffline(ctx context.Context, args []string) int {
	if len(args) < 3 {
		return c.adminUsageError("usage: opencook admin orgs add-user ORG USER --offline --yes [--admin]\n       opencook admin orgs remove-user ORG USER --offline --yes [--force]\n\n")
	}

	action, orgName, username := args[0], args[1], args[2]
	fs := flag.NewFlagSet("opencook admin orgs "+action, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := bindOfflineMutationFlags(fs)
	adminUser := fs.Bool("admin", false, "also add the user to the admins group")
	force := fs.Bool("force", false, "allow removal even if the user record is missing")
	if err := fs.Parse(args[3:]); err != nil {
		return c.adminFlagError("admin orgs "+action, err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin orgs %s received unexpected arguments: %v\n\n", action, fs.Args())
	}
	if !opts.offline || !opts.yes {
		return c.adminUsageError("admin orgs %s is offline-only and requires --offline --yes\n\n", action)
	}

	return c.runOfflineBootstrapMutation(ctx, opts.postgresDSN, func(state bootstrap.BootstrapCoreState) (bootstrap.BootstrapCoreState, map[string]any, error) {
		switch action {
		case "add-user":
			next, changed, err := bootstrap.AddUserToBootstrapCoreOrg(state, orgName, username, *adminUser)
			return next, offlineMembershipResponse("org-user-add", changed), err
		case "remove-user":
			next, changed, err := bootstrap.RemoveUserFromBootstrapCoreOrg(state, orgName, username, *force)
			return next, offlineMembershipResponse("org-user-remove", changed), err
		default:
			return state, nil, fmt.Errorf("%w: unsupported org membership action", bootstrap.ErrInvalidInput)
		}
	})
}

func (c *command) runAdminGroupMembershipOffline(ctx context.Context, args []string) int {
	if len(args) < 4 {
		return c.adminUsageError("usage: opencook admin groups add-actor ORG GROUP ACTOR --offline --yes [--actor-type user|client|group]\n       opencook admin groups remove-actor ORG GROUP ACTOR --offline --yes [--actor-type user|client|group]\n\n")
	}

	action, orgName, groupName, actorName := args[0], args[1], args[2], args[3]
	fs := flag.NewFlagSet("opencook admin groups "+action, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := bindOfflineMutationFlags(fs)
	actorType := fs.String("actor-type", "user", "actor type: user, client, or group")
	if err := fs.Parse(args[4:]); err != nil {
		return c.adminFlagError("admin groups "+action, err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin groups %s received unexpected arguments: %v\n\n", action, fs.Args())
	}
	if !opts.offline || !opts.yes {
		return c.adminUsageError("admin groups %s is offline-only and requires --offline --yes\n\n", action)
	}

	return c.runOfflineBootstrapMutation(ctx, opts.postgresDSN, func(state bootstrap.BootstrapCoreState) (bootstrap.BootstrapCoreState, map[string]any, error) {
		switch action {
		case "add-actor":
			next, changed, err := bootstrap.AddActorToBootstrapCoreGroup(state, orgName, groupName, *actorType, actorName)
			return next, offlineMembershipResponse("group-actor-add", changed), err
		case "remove-actor":
			next, changed, err := bootstrap.RemoveActorFromBootstrapCoreGroup(state, orgName, groupName, *actorType, actorName)
			return next, offlineMembershipResponse("group-actor-remove", changed), err
		default:
			return state, nil, fmt.Errorf("%w: unsupported group membership action", bootstrap.ErrInvalidInput)
		}
	})
}

func (c *command) runAdminServerAdminsOffline(ctx context.Context, args []string) int {
	if len(args) == 0 {
		return c.adminUsageError("admin server-admins requires list, grant, or revoke\n\n")
	}

	switch args[0] {
	case "list":
		fs := flag.NewFlagSet("opencook admin server-admins list", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		opts := bindOfflineReadFlags(fs)
		if err := fs.Parse(args[1:]); err != nil {
			return c.adminFlagError("admin server-admins list", err)
		}
		if fs.NArg() != 0 {
			return c.adminUsageError("admin server-admins list received unexpected arguments: %v\n\n", fs.Args())
		}
		if !opts.offline {
			return c.adminUsageError("admin server-admins list is offline-only and requires --offline\n\n")
		}
		store, closeStore, code, ok := c.openOfflineStore(ctx, opts.postgresDSN)
		if !ok {
			return code
		}
		defer closeOfflineStore(closeStore)
		state, err := store.LoadBootstrapCore()
		if err != nil {
			return c.offlineError("load bootstrap core", err)
		}
		return c.writeOfflineResult(map[string]any{
			"mode":          "offline",
			"server_admins": bootstrap.ListBootstrapServerAdmins(state),
			"restart_note":  offlineRestartNote,
		})
	case "grant", "revoke":
		if len(args) < 2 {
			return c.adminUsageError("usage: opencook admin server-admins %s USER --offline --yes\n\n", args[0])
		}
		action, username := args[0], args[1]
		fs := flag.NewFlagSet("opencook admin server-admins "+action, flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		opts := bindOfflineMutationFlags(fs)
		if err := fs.Parse(args[2:]); err != nil {
			return c.adminFlagError("admin server-admins "+action, err)
		}
		if fs.NArg() != 0 {
			return c.adminUsageError("admin server-admins %s received unexpected arguments: %v\n\n", action, fs.Args())
		}
		if !opts.offline || !opts.yes {
			return c.adminUsageError("admin server-admins %s is offline-only and requires --offline --yes\n\n", action)
		}
		return c.runOfflineBootstrapMutation(ctx, opts.postgresDSN, func(state bootstrap.BootstrapCoreState) (bootstrap.BootstrapCoreState, map[string]any, error) {
			switch action {
			case "grant":
				next, changed, err := bootstrap.GrantBootstrapServerAdmin(state, username)
				return next, offlineMembershipResponse("server-admin-grant", changed), err
			case "revoke":
				next, changed, err := bootstrap.RevokeBootstrapServerAdmin(state, username)
				return next, offlineMembershipResponse("server-admin-revoke", changed), err
			default:
				return state, nil, fmt.Errorf("%w: unsupported server-admin action", bootstrap.ErrInvalidInput)
			}
		})
	default:
		return c.adminUsageError("unknown admin server-admins command %q\n\n", args[0])
	}
}

func (c *command) runAdminACLRepairOffline(ctx context.Context, args []string) int {
	if len(args) == 0 || args[0] != "repair-defaults" {
		return c.adminUsageError("usage: opencook admin acls repair-defaults --offline [--org ORG] [--dry-run|--yes]\n\n")
	}
	fs := flag.NewFlagSet("opencook admin acls repair-defaults", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := bindOfflineMutationFlags(fs)
	dryRun := fs.Bool("dry-run", false, "report missing default ACLs without saving")
	orgName := fs.String("org", "", "limit repair to one organization")
	superuser := fs.String("superuser", "pivotal", "superuser name for repaired ACL defaults")
	if err := fs.Parse(args[1:]); err != nil {
		return c.adminFlagError("admin acls repair-defaults", err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin acls repair-defaults received unexpected arguments: %v\n\n", fs.Args())
	}
	if !opts.offline {
		return c.adminUsageError("admin acls repair-defaults is offline-only and requires --offline\n\n")
	}
	if !*dryRun && !opts.yes {
		return c.adminUsageError("admin acls repair-defaults requires --dry-run or --yes\n\n")
	}
	orgFilter := strings.TrimSpace(*orgName)

	store, closeStore, code, ok := c.openOfflineStore(ctx, opts.postgresDSN)
	if !ok {
		return code
	}
	defer closeOfflineStore(closeStore)

	core, err := store.LoadBootstrapCore()
	if err != nil {
		return c.offlineError("load bootstrap core", err)
	}
	if orgFilter != "" {
		if _, ok := core.Orgs[orgFilter]; !ok {
			return c.offlineError("repair ACL defaults", fmt.Errorf("%w: organization %s not found", bootstrap.ErrNotFound, orgFilter))
		}
	}
	objects, err := store.LoadCoreObjects()
	if err != nil {
		return c.offlineError("load core objects", err)
	}

	nextCore, coreRepair := bootstrap.RepairBootstrapCoreDefaultACLs(core, orgFilter, *superuser)
	nextObjects, objectRepair := bootstrap.RepairCoreObjectDefaultACLs(objects, orgFilter, *superuser)
	if !*dryRun {
		if err := store.SaveBootstrapCore(nextCore); err != nil {
			return c.offlineError("save bootstrap core", err)
		}
		if err := store.SaveCoreObjects(nextObjects); err != nil {
			return c.offlineError("save core objects", err)
		}
	}

	return c.writeOfflineResult(map[string]any{
		"mode":                      "offline",
		"dry_run":                   *dryRun,
		"changed":                   coreRepair.Changed || objectRepair.Changed,
		"bootstrap_repaired_acls":   coreRepair.Repaired,
		"core_object_repaired_acls": objectRepair.Repaired,
		"restart_note":              offlineRestartNote,
	})
}

type adminOfflineFlagValues struct {
	offline     bool
	yes         bool
	postgresDSN string
}

func bindOfflineMutationFlags(fs *flag.FlagSet) *adminOfflineFlagValues {
	opts := &adminOfflineFlagValues{}
	fs.BoolVar(&opts.offline, "offline", false, "allow direct PostgreSQL mutation while OpenCook servers are stopped")
	fs.BoolVar(&opts.yes, "yes", false, "confirm the offline mutation")
	fs.StringVar(&opts.postgresDSN, "postgres-dsn", "", "PostgreSQL DSN; defaults to OPENCOOK_POSTGRES_DSN")
	return opts
}

func bindOfflineReadFlags(fs *flag.FlagSet) *adminOfflineFlagValues {
	opts := &adminOfflineFlagValues{}
	fs.BoolVar(&opts.offline, "offline", false, "allow direct PostgreSQL read")
	fs.StringVar(&opts.postgresDSN, "postgres-dsn", "", "PostgreSQL DSN; defaults to OPENCOOK_POSTGRES_DSN")
	return opts
}

func (c *command) runOfflineBootstrapMutation(ctx context.Context, dsn string, mutate func(bootstrap.BootstrapCoreState) (bootstrap.BootstrapCoreState, map[string]any, error)) int {
	store, closeStore, code, ok := c.openOfflineStore(ctx, dsn)
	if !ok {
		return code
	}
	defer closeOfflineStore(closeStore)

	state, err := store.LoadBootstrapCore()
	if err != nil {
		return c.offlineError("load bootstrap core", err)
	}
	next, response, err := mutate(state)
	if err != nil {
		return c.offlineError("prepare offline mutation", err)
	}
	if err := store.SaveBootstrapCore(next); err != nil {
		return c.offlineError("save bootstrap core", err)
	}
	response["mode"] = "offline"
	response["restart_note"] = offlineRestartNote
	return c.writeOfflineResult(response)
}

func (c *command) openOfflineStore(ctx context.Context, dsn string) (adminOfflineStore, func() error, int, bool) {
	cfg, err := c.loadOffline()
	if err != nil {
		fmt.Fprintf(c.stderr, "load offline config: %v\n", err)
		return nil, nil, exitDependencyUnavailable, false
	}
	if strings.TrimSpace(dsn) == "" {
		dsn = cfg.PostgresDSN
	}
	store, closeStore, err := c.newOfflineStore(ctx, dsn)
	if err != nil {
		fmt.Fprintf(c.stderr, "open offline store: %v\n", err)
		return nil, nil, exitDependencyUnavailable, false
	}
	return store, closeStore, exitOK, true
}

func closeOfflineStore(closeStore func() error) {
	if closeStore != nil {
		_ = closeStore()
	}
}

func (c *command) offlineError(prefix string, err error) int {
	fmt.Fprintf(c.stderr, "%s: %v\n", prefix, err)
	if errors.Is(err, bootstrap.ErrNotFound) {
		return exitNotFound
	}
	if errors.Is(err, bootstrap.ErrInvalidInput) {
		return exitUsage
	}
	return exitDependencyUnavailable
}

func (c *command) writeOfflineResult(response map[string]any) int {
	if err := writePrettyJSON(c.stdout, response); err != nil {
		fmt.Fprintf(c.stderr, "write admin output: %v\n", err)
		return exitDependencyUnavailable
	}
	return exitOK
}

func offlineMembershipResponse(operation string, changed []string) map[string]any {
	if changed == nil {
		changed = []string{}
	}
	return map[string]any{
		"operation": operation,
		"changed":   len(changed) > 0,
		"members":   changed,
	}
}

func adminACLPath(args []string) (string, bool) {
	switch {
	case len(args) == 2 && args[0] == "user":
		return adminPath("users", args[1], "_acl"), true
	case len(args) == 2 && (args[0] == "org" || args[0] == "organization"):
		return adminPath("organizations", args[1], "_acl"), true
	case len(args) == 3 && args[0] == "group":
		return adminPath("organizations", args[1], "groups", args[2], "_acl"), true
	case len(args) == 3 && args[0] == "container":
		return adminPath("organizations", args[1], "containers", args[2], "_acl"), true
	case len(args) == 3 && args[0] == "client":
		return adminPath("organizations", args[1], "clients", args[2], "_acl"), true
	default:
		return "", false
	}
}

const offlineRestartNote = "offline PostgreSQL changes are not visible to running OpenCook servers until those processes are restarted"
