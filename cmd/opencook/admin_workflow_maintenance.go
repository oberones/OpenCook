package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	errAdminMaintenanceRequired    = errors.New("active maintenance mode is required")
	errAdminMaintenanceCheckFailed = errors.New("maintenance state check failed")
)

const adminWorkflowMaintenanceRequiredMessage = "active maintenance mode is required before mutating online derived state; run opencook admin maintenance enable --mode repair --reason TEXT --yes first"

// requireAdminWorkflowMaintenance checks the shared maintenance gate without
// mutating it. Online repair-style commands use this to avoid acquiring a
// temporary gate they might fail to release if the process is interrupted.
func (c *command) requireAdminWorkflowMaintenance(ctx context.Context, postgresDSN, workflow string) ([]string, error) {
	store, backend, closeStore, err := c.openAdminMaintenanceStore(ctx, postgresDSN)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errAdminMaintenanceCheckFailed, err)
	}
	defer closeAdminMaintenanceStore(closeStore)

	check, err := store.Check(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errAdminMaintenanceCheckFailed, err)
	}
	if !check.Active {
		return nil, errAdminMaintenanceRequired
	}

	backendName := strings.TrimSpace(backend.Name)
	if backendName == "" {
		backendName = "unknown"
	}
	warnings := []string{fmt.Sprintf("active maintenance mode confirmed for %s using %s-backed state; keep maintenance enabled until the workflow and follow-up checks finish", workflow, backendName)}
	warnings = append(warnings, adminMaintenanceBackendWarnings(backend)...)
	return warnings, nil
}

// appendUniqueAdminWarnings preserves caller ordering while avoiding duplicate
// advisory text when a workflow gathers warnings from several subsystems.
func appendUniqueAdminWarnings(base []string, extra ...string) []string {
	seen := make(map[string]struct{}, len(base)+len(extra))
	out := make([]string, 0, len(base)+len(extra))
	for _, warning := range append(append([]string(nil), base...), extra...) {
		warning = strings.TrimSpace(warning)
		if warning == "" {
			continue
		}
		if _, ok := seen[warning]; ok {
			continue
		}
		seen[warning] = struct{}{}
		out = append(out, warning)
	}
	return out
}
