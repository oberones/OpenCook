package main

import (
	"context"
	"fmt"
	"io"
	"strings"
)

type adminRunbookListOutput struct {
	OK       bool                   `json:"ok"`
	Command  string                 `json:"command"`
	Runbooks []adminRunbookListItem `json:"runbooks"`
}

type adminRunbookShowOutput struct {
	OK      bool         `json:"ok"`
	Command string       `json:"command"`
	Runbook adminRunbook `json:"runbook"`
}

type adminRunbookListItem struct {
	Name    string `json:"name"`
	Title   string `json:"title"`
	Summary string `json:"summary"`
}

type adminRunbook struct {
	Name              string                         `json:"name"`
	Title             string                         `json:"title"`
	Summary           string                         `json:"summary"`
	Commands          []adminRunbookCommand          `json:"commands,omitempty"`
	ServiceManagement []adminRunbookServicePattern   `json:"service_management,omitempty"`
	Notes             []string                       `json:"notes,omitempty"`
	Unsupported       []adminRunbookUnsupportedEntry `json:"unsupported,omitempty"`
	Docs              []string                       `json:"docs,omitempty"`
}

type adminRunbookCommand struct {
	Command string `json:"command"`
	Purpose string `json:"purpose"`
	Notes   string `json:"notes,omitempty"`
}

type adminRunbookServicePattern struct {
	Supervisor string   `json:"supervisor"`
	Pattern    string   `json:"pattern"`
	Commands   []string `json:"commands,omitempty"`
	Notes      []string `json:"notes,omitempty"`
}

type adminRunbookUnsupportedEntry struct {
	Workflow string `json:"workflow"`
	Reason   string `json:"reason"`
}

// runAdminRunbook dispatches read-only operational runbook discovery. It does
// not contact OpenCook, PostgreSQL, OpenSearch, or blob providers, so the output
// is safe to generate even while a deployment is degraded.
func (c *command) runAdminRunbook(_ context.Context, args []string, inheritedJSON bool) int {
	if len(args) == 0 {
		return c.adminUsageError("admin runbook requires list or show\n\n")
	}

	switch args[0] {
	case "list":
		return c.runAdminRunbookList(args[1:], inheritedJSON)
	case "show":
		return c.runAdminRunbookShow(args[1:], inheritedJSON)
	case "help", "-h", "--help":
		c.printAdminRunbookUsage(c.stdout)
		return exitOK
	default:
		return c.adminUsageError("unknown admin runbook command %q\n\n", args[0])
	}
}

// runAdminRunbookList prints the available runbook names and summaries in a
// stable JSON shape suitable for shell completion and support tooling.
func (c *command) runAdminRunbookList(args []string, inheritedJSON bool) int {
	rest, jsonOutput, err := parseAdminRunbookJSONFlag(args, inheritedJSON)
	if err != nil {
		return c.adminFlagError("admin runbook list", err)
	}
	if len(rest) != 0 {
		return c.adminUsageError("admin runbook list received unexpected arguments: %v\n\n", rest)
	}
	_ = jsonOutput

	out := adminRunbookListOutput{
		OK:       true,
		Command:  "runbook_list",
		Runbooks: adminRunbookListItems(),
	}
	if err := writePrettyJSON(c.stdout, out); err != nil {
		fmt.Fprintf(c.stderr, "write runbook list output: %v\n", err)
		return exitDependencyUnavailable
	}
	return exitOK
}

// runAdminRunbookShow prints one named runbook with command references and
// deployment notes. Names are normalized so operators can use hyphens or
// underscores interchangeably.
func (c *command) runAdminRunbookShow(args []string, inheritedJSON bool) int {
	rest, jsonOutput, err := parseAdminRunbookJSONFlag(args, inheritedJSON)
	if err != nil {
		return c.adminFlagError("admin runbook show", err)
	}
	if len(rest) != 1 {
		return c.adminUsageError("usage: opencook admin runbook show NAME [--json]\n\n")
	}
	_ = jsonOutput

	runbook, ok := adminRunbookByName(rest[0])
	if !ok {
		return c.adminUsageError("unknown admin runbook %q\n\n", rest[0])
	}
	out := adminRunbookShowOutput{
		OK:      true,
		Command: "runbook_show",
		Runbook: runbook,
	}
	if err := writePrettyJSON(c.stdout, out); err != nil {
		fmt.Fprintf(c.stderr, "write runbook output: %v\n", err)
		return exitDependencyUnavailable
	}
	return exitOK
}

// parseAdminRunbookJSONFlag accepts --json before or after positional names so
// the CLI matches the documented `runbook show NAME [--json]` shape.
func parseAdminRunbookJSONFlag(args []string, inheritedJSON bool) ([]string, bool, error) {
	jsonOutput := inheritedJSON
	rest := make([]string, 0, len(args))
	for _, arg := range args {
		switch arg {
		case "--json", "-json":
			jsonOutput = true
		default:
			if strings.HasPrefix(arg, "-") {
				return nil, jsonOutput, fmt.Errorf("flag provided but not defined: %s", arg)
			}
			rest = append(rest, arg)
		}
	}
	return rest, jsonOutput, nil
}

// adminRunbookCatalog is the single in-process source for runbook command
// discovery and diagnostics bundle metadata. Keep entries free of concrete
// secrets, DSNs, tokens, hostnames with credentials, and private key paths.
func adminRunbookCatalog() []adminRunbook {
	docs := []string{
		"docs/chef-server-ctl-operational-runbooks.md",
		"docs/chef-server-ctl-operational-parity-plan.md",
	}
	runbooks := []adminRunbook{
		{
			Name:    "service-management",
			Title:   "Service Management",
			Summary: "Run OpenCook under an external process supervisor and use admin commands for health checks.",
			ServiceManagement: []adminRunbookServicePattern{
				{
					Supervisor: "systemd",
					Pattern:    "package or binary deployments should let systemd own start, stop, restart, log retention, and dependency ordering",
					Commands: []string{
						"systemctl status opencook",
						"systemctl restart opencook",
						"journalctl -u opencook",
					},
					Notes: []string{
						"run opencook admin config check before restarting after config changes",
						"use opencook admin service doctor --offline only during a maintenance window when direct PostgreSQL inspection is intended",
					},
				},
				{
					Supervisor: "docker-compose",
					Pattern:    "compose deployments should run opencook, postgres, opensearch, and provider-compatible services on a shared network",
					Commands: []string{
						"docker compose ps",
						"docker compose logs opencook",
						"docker compose restart opencook",
					},
					Notes: []string{
						"keep provider credentials in compose environment or secret files, not in diagnostics bundles",
						"use scripts/functional-compose.sh operational for the repo's functional smoke path",
					},
				},
				{
					Supervisor: "kubernetes",
					Pattern:    "Kubernetes deployments should use readiness probes, liveness probes, service logs, and rollout controls",
					Commands: []string{
						"kubectl rollout status deployment/opencook",
						"kubectl logs deployment/opencook",
						"kubectl rollout restart deployment/opencook",
					},
					Notes: []string{
						"point readiness probes at /readyz and metrics scrapers at /metrics",
						"store PostgreSQL, OpenSearch, and blob credentials in Kubernetes Secrets or an external secret manager",
					},
				},
			},
			Commands: []adminRunbookCommand{
				{Command: "opencook admin config check --json", Purpose: "validate local OPENCOOK_* settings before starting or restarting the server"},
				{Command: "opencook admin service status --json", Purpose: "summarize local service configuration without probing providers"},
				{Command: "opencook admin service doctor --offline --json", Purpose: "run deeper non-mutating diagnostics with explicit offline PostgreSQL inspection"},
			},
			Notes: []string{
				"OpenCook does not embed an omnibus-style service supervisor; process lifecycle belongs to the deployment platform.",
				"Chef-facing route behavior should not change during service-management work.",
			},
		},
		{
			Name:    "observability",
			Title:   "Observability And Diagnostics",
			Summary: "Collect safe operational context without copying private keys, signatures, database dumps, blob objects, or raw log files.",
			Commands: []adminRunbookCommand{
				{Command: "opencook admin logs paths --json", Purpose: "show stdout/stderr and external supervisor log discovery references"},
				{Command: "opencook admin diagnostics collect --output PATH --yes --json", Purpose: "create a redacted diagnostics bundle with config, status, dependency, and runbook summaries"},
				{Command: "curl -fsS http://HOST:PORT/metrics", Purpose: "scrape Prometheus-compatible metrics from a running OpenCook process"},
				{Command: "curl -fsS http://HOST:PORT/readyz", Purpose: "check runtime readiness without requiring a signed Chef request"},
			},
			Notes: []string{
				"diagnostics bundles include log references, not copied log contents",
				"request IDs are returned in X-Request-Id and included in structured request logs",
			},
		},
		{
			Name:    "backup-restore",
			Title:   "Backup And Restore",
			Summary: "Use the migration command family for logical backup inspection, preflight, and restore workflows.",
			Commands: []adminRunbookCommand{
				{Command: "opencook admin migration backup create --output PATH --offline --yes --json", Purpose: "create a logical backup bundle during an explicit offline maintenance window"},
				{Command: "opencook admin migration backup inspect PATH --json", Purpose: "verify manifest and payload hashes before restore or handoff"},
				{Command: "opencook admin migration restore preflight PATH --offline --json", Purpose: "validate a restore target before applying backup data"},
				{Command: "opencook admin migration restore apply PATH --offline --yes --json", Purpose: "apply a validated backup into an offline target"},
			},
			Notes: []string{
				"backup/restore is intentionally separate from diagnostics collection",
				"restore applies to OpenCook logical backup bundles, not raw PostgreSQL dumps or arbitrary Chef Server internals",
			},
		},
		{
			Name:    "search-reindex",
			Title:   "Search Consistency And Reindex",
			Summary: "Check and repair OpenSearch documents from PostgreSQL-backed state.",
			Commands: []adminRunbookCommand{
				{Command: "opencook admin search check --all-orgs --json", Purpose: "compare indexed documents with PostgreSQL-backed source state"},
				{Command: "opencook admin search repair --all-orgs --dry-run --json", Purpose: "preview OpenSearch repairs without mutations"},
				{Command: "opencook admin search repair --all-orgs --yes --json", Purpose: "repair mismatched or missing OpenSearch documents"},
				{Command: "opencook admin reindex --all-orgs --complete --json", Purpose: "drop and rebuild searchable documents from PostgreSQL-backed state"},
			},
			Notes: []string{
				"complete reindex can race with concurrent writes; prefer a maintenance window until a future maintenance gate exists",
				"unsupported search indexes remain intentionally rejected rather than silently fabricated",
			},
		},
		{
			Name:    "migration-cutover",
			Title:   "Migration And Cutover Rehearsal",
			Summary: "Use preflight, source import/sync, restored-target reindex, shadow comparison, and cutover rehearsal before switching clients.",
			Commands: []adminRunbookCommand{
				{Command: "opencook admin migration preflight --all-orgs --json", Purpose: "summarize migration readiness against configured OpenCook state"},
				{Command: "opencook admin migration source inventory PATH --json", Purpose: "inventory source artifacts without requiring live source mutation"},
				{Command: "opencook admin migration source normalize PATH --output normalized-source --yes --json", Purpose: "write deterministic normalized source payloads and copied blob references"},
				{Command: "opencook admin migration source import preflight normalized-source --offline --json", Purpose: "validate source input and target safety before import"},
				{Command: "opencook admin migration source import apply normalized-source --offline --yes --progress source-import-progress.json --json", Purpose: "apply normalized source metadata and copied blobs to an offline target"},
				{Command: "opencook admin migration source sync preflight normalized-source --offline --progress source-sync-progress.json --json", Purpose: "preview a repeat source snapshot before final cutover sync"},
				{Command: "opencook admin migration source sync apply normalized-source --offline --yes --progress source-sync-progress.json --json", Purpose: "apply one final source snapshot after freezing source writes"},
				{Command: "opencook admin reindex --all-orgs --complete --json", Purpose: "rebuild restored target OpenSearch documents from PostgreSQL"},
				{Command: "opencook admin search check --all-orgs --json", Purpose: "capture clean search evidence for the cutover gate"},
				{Command: "opencook admin migration shadow compare --source normalized-source --target-server-url URL --json", Purpose: "capture read-only source/target comparison evidence"},
				{Command: "opencook admin migration cutover rehearse --manifest PATH --source normalized-source --source-import-progress source-import-progress.json --source-sync-progress source-sync-progress.json --search-check-result PATH --shadow-result PATH --rollback-ready --server-url URL --json", Purpose: "gate client cutover using import, sync, search, shadow-read, auth, blob, and rollback evidence"},
			},
			Notes: []string{
				"freeze source Chef writes before the final source sync; do not proxy source writes into OpenCook during rehearsal",
				"switch DNS/load balancers or Chef/Cinc chef_server_url only after blocker gates pass",
				"keep the source Chef Infra Server read/write path available until post-cutover smoke checks pass",
				"treat cutover rehearsal errors as blockers and warnings as advisories that require an explicit operator decision",
			},
		},
		{
			Name:    "identity-acl-repair",
			Title:   "Identity, Group, Container, And ACL Repair",
			Summary: "Use live-safe admin reads and explicitly offline repair commands for identity and authorization state.",
			Commands: []adminRunbookCommand{
				{Command: "opencook admin users list --json", Purpose: "inspect users through the live signed API path"},
				{Command: "opencook admin orgs list --json", Purpose: "inspect organizations through the live signed API path"},
				{Command: "opencook admin acls repair-defaults --offline --dry-run --json", Purpose: "preview default ACL repairs with direct PostgreSQL inspection"},
				{Command: "opencook admin acls repair-defaults --offline --yes --json", Purpose: "apply default ACL repairs during an offline maintenance window"},
			},
			Notes: []string{
				"unsafe direct mutations stay gated by --offline and --yes",
				"live-safe reads prefer signed HTTP so the running service remains the compatibility boundary",
			},
		},
		{
			Name:    "unsupported-omnibus",
			Title:   "Unsupported Omnibus Workflows",
			Summary: "Document upstream chef-server-ctl workflows that OpenCook intentionally delegates to deployment tooling or excludes.",
			Unsupported: []adminRunbookUnsupportedEntry{
				{Workflow: "embedded process supervisor", Reason: "OpenCook is a single server process managed by systemd, Docker Compose, Kubernetes, launchd, or another external supervisor"},
				{Workflow: "omnibus reconfigure", Reason: "configuration is environment-driven; use opencook admin config check and restart through the supervisor"},
				{Workflow: "licensing and license telemetry", Reason: "OpenCook is Apache-2.0 software and intentionally has no licensing subsystem or license-management endpoints"},
				{Workflow: "maintenance-mode traffic blocking", Reason: "live request blocking changes Chef-facing traffic semantics and needs a separate compatibility design"},
				{Workflow: "interactive psql shell wrapper", Reason: "direct database access remains an operator/platform concern; offline repair commands expose the safe supported mutations"},
				{Workflow: "secret rotation helpers", Reason: "provider credentials remain deployment/secret-manager concerns until OpenCook has a formal secret-store abstraction"},
			},
			Notes: []string{
				"unsupported does not mean forgotten; these workflows are either intentionally outside OpenCook's product stance or deferred to future compatibility design.",
			},
		},
	}
	for i := range runbooks {
		runbooks[i].Docs = append([]string(nil), docs...)
	}
	return runbooks
}

// adminRunbookListItems returns the stable list view derived from the full
// catalog so list/show/diagnostics cannot disagree about available runbooks.
func adminRunbookListItems() []adminRunbookListItem {
	runbooks := adminRunbookCatalog()
	items := make([]adminRunbookListItem, 0, len(runbooks))
	for _, runbook := range runbooks {
		items = append(items, adminRunbookListItem{
			Name:    runbook.Name,
			Title:   runbook.Title,
			Summary: runbook.Summary,
		})
	}
	return items
}

// adminRunbookByName finds a runbook using a forgiving normalized key while
// preserving the canonical name in the returned payload.
func adminRunbookByName(name string) (adminRunbook, bool) {
	wanted := normalizeAdminRunbookName(name)
	for _, runbook := range adminRunbookCatalog() {
		if normalizeAdminRunbookName(runbook.Name) == wanted {
			return runbook, true
		}
	}
	return adminRunbook{}, false
}

// normalizeAdminRunbookName makes CLI lookup resilient to hyphen/underscore
// differences without accepting arbitrary aliases that could surprise operators.
func normalizeAdminRunbookName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.ReplaceAll(name, "_", "-")
	return name
}

// adminDiagnosticsRunbooks exposes the same safe catalog inside diagnostics
// bundles so support artifacts include command references without environment
// values, credentials, or live API responses.
func adminDiagnosticsRunbooks() []adminRunbook {
	return adminRunbookCatalog()
}

// printAdminRunbookUsage documents local runbook discovery separately from
// commands that inspect or mutate operational state.
func (c *command) printAdminRunbookUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  opencook admin runbook list [--json]
  opencook admin runbook show NAME [--json]

Discover OpenCook operational runbooks for service management, observability,
backup/restore, reindex/search repair, migration/cutover, identity/ACL repair,
and intentionally unsupported omnibus workflows.
`)
}
