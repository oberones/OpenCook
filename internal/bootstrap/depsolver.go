package bootstrap

import (
	"fmt"
	"sort"
	"strings"
)

type DepsolverError struct {
	Detail map[string]any
}

func (e *DepsolverError) Error() string {
	if e == nil {
		return ""
	}
	if message, ok := e.Detail["message"].(string); ok {
		return message
	}
	return "depsolver failed"
}

type depsolverRunListItem struct {
	Cookbook   string
	Constraint string
	Label      string
}

type depsolverConstraintSource struct {
	Constraint string
	Source     string
}

type depsolverSolution struct {
	Versions     map[string]CookbookVersion
	Constraints  map[string][]depsolverConstraintSource
	RootMessages map[string]string
}

func (s *Service) SolveEnvironmentCookbookVersions(orgName, environmentName string, payload map[string]any) (map[string]CookbookVersion, bool, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false, false, nil
	}
	env, ok := org.envs[environmentName]
	if !ok {
		return nil, true, false, nil
	}

	runList, err := validateDepsolverPayload(payload)
	if err != nil {
		return nil, true, true, err
	}
	if len(runList) == 0 {
		return map[string]CookbookVersion{}, true, true, nil
	}

	items := make([]depsolverRunListItem, 0, len(runList))
	nonExistent := make(map[string]struct{})
	noVersions := make([]string, 0)
	for _, entry := range runList {
		item, err := parseDepsolverRunListItem(entry)
		if err != nil {
			return nil, true, true, &ValidationError{Messages: []string{"Field 'run_list' is not a valid run list"}}
		}

		versions, exists := org.cookbooks[item.Cookbook]
		if !exists {
			nonExistent[item.Cookbook] = struct{}{}
			continue
		}

		refs := cookbookVersionRefs(versions)
		refs = filterEnvironmentCookbookRefs(refs, env.CookbookVersions[item.Cookbook])
		refs = filterDepsolverCookbookRefs(refs, []string{item.Constraint})
		if len(refs) == 0 {
			noVersions = append(noVersions, item.Label)
			continue
		}

		items = append(items, item)
	}

	if len(nonExistent) > 0 {
		missing := make([]string, 0, len(nonExistent))
		for name := range nonExistent {
			missing = append(missing, name)
		}
		sort.Strings(missing)
		return nil, true, true, rootMissingCookbooksError(missing)
	}
	if len(noVersions) > 0 {
		sort.Strings(noVersions)
		return nil, true, true, rootNoVersionsError(noVersions)
	}

	solution := depsolverSolution{
		Versions:     make(map[string]CookbookVersion),
		Constraints:  make(map[string][]depsolverConstraintSource),
		RootMessages: make(map[string]string),
	}
	for _, item := range items {
		if _, exists := solution.RootMessages[item.Cookbook]; !exists {
			solution.RootMessages[item.Cookbook] = item.Label
		}
		next, err := solveDepsolverCookbook(org, env, solution, item.Cookbook, item.Cookbook, depsolverConstraintSource{
			Constraint: item.Constraint,
		})
		if err != nil {
			return nil, true, true, err
		}
		solution = next
	}

	out := make(map[string]CookbookVersion, len(solution.Versions))
	for name, version := range solution.Versions {
		out[name] = copyCookbookVersion(version)
	}
	return out, true, true, nil
}

func validateDepsolverPayload(payload map[string]any) ([]string, error) {
	if payload == nil {
		return nil, &ValidationError{Messages: []string{"Field 'run_list' is not a valid run list"}}
	}

	runListValue, ok := payload["run_list"]
	if !ok {
		return nil, &ValidationError{Messages: []string{"Field 'run_list' is not a valid run list"}}
	}
	return validateDepsolverRunList(runListValue)
}

func validateDepsolverRunList(value any) ([]string, error) {
	runList, err := validateRunList(value)
	if err != nil {
		return nil, err
	}

	for _, item := range runList {
		if validRoleRunListPattern.MatchString(item) {
			return nil, &ValidationError{Messages: []string{
				fmt.Sprintf("Field 'run_list' contains unsupported role item %s", item),
			}}
		}
	}

	return runList, nil
}

func parseDepsolverRunListItem(value string) (depsolverRunListItem, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return depsolverRunListItem{}, ErrInvalidInput
	}

	token := value
	switch {
	case strings.HasPrefix(value, "recipe[") && strings.HasSuffix(value, "]"):
		token = strings.TrimSuffix(strings.TrimPrefix(value, "recipe["), "]")
	case strings.HasPrefix(value, "role[") && strings.HasSuffix(value, "]"):
		return depsolverRunListItem{}, ErrInvalidInput
	}

	versionConstraint := ">= 0.0.0"
	if cookbookToken, versionToken, ok := strings.Cut(token, "@"); ok {
		token = cookbookToken
		versionToken = strings.TrimSpace(versionToken)
		if !validCookbookVersion(versionToken) {
			return depsolverRunListItem{}, ErrInvalidInput
		}
		versionConstraint = "= " + versionToken
	}

	cookbookName := token
	if idx := strings.Index(cookbookName, "::"); idx >= 0 {
		cookbookName = cookbookName[:idx]
	}
	cookbookName = strings.TrimSpace(cookbookName)
	if !validCookbookNamePattern.MatchString(cookbookName) {
		return depsolverRunListItem{}, ErrInvalidInput
	}

	return depsolverRunListItem{
		Cookbook:   cookbookName,
		Constraint: versionConstraint,
		Label:      fmt.Sprintf("(%s %s)", cookbookName, versionConstraint),
	}, nil
}

func solveDepsolverCookbook(org *organizationState, env Environment, current depsolverSolution, cookbook, rootCookbook string, incoming depsolverConstraintSource) (depsolverSolution, error) {
	next := cloneDepsolverSolution(current)
	next.Constraints[cookbook] = append(next.Constraints[cookbook], incoming)

	versions, exists := org.cookbooks[cookbook]
	if !exists {
		return next, missingDependencyCookbookError(cookbook, next.RootMessages[rootCookbook], depsolverConstraintSources(next.Constraints[cookbook]))
	}

	refs := cookbookVersionRefs(versions)
	refs = filterEnvironmentCookbookRefs(refs, env.CookbookVersions[cookbook])
	refs = filterDepsolverCookbookRefs(refs, depsolverConstraintValues(next.Constraints[cookbook]))

	if selected, ok := next.Versions[cookbook]; ok && depsolverVersionMatchesAll(selected.Version, next.Constraints[cookbook]) {
		return next, nil
	}

	var lastErr error
	for _, ref := range refs {
		candidate := versions[ref.Version]
		trial := cloneDepsolverSolution(next)
		trial.Versions[cookbook] = copyCookbookVersion(candidate)

		dependencies := cookbookMetadataDependencies(candidate.Metadata)
		depNames := make([]string, 0, len(dependencies))
		for name := range dependencies {
			depNames = append(depNames, name)
		}
		sort.Strings(depNames)

		dependencyFailed := false
		for _, depName := range depNames {
			depConstraint := strings.TrimSpace(dependencies[depName])
			if depConstraint == "" {
				depConstraint = ">= 0.0.0"
			}

			var err error
			trial, err = solveDepsolverCookbook(org, env, trial, depName, rootCookbook, depsolverConstraintSource{
				Constraint: depConstraint,
				Source:     fmt.Sprintf("(%s = %s) -> (%s %s)", candidate.CookbookName, candidate.Version, depName, depConstraint),
			})
			if err != nil {
				lastErr = err
				dependencyFailed = true
				break
			}
		}
		if dependencyFailed {
			continue
		}

		return trial, nil
	}

	if lastErr != nil {
		return next, lastErr
	}
	return next, unsatisfiedDependencyCookbookError(cookbook, versions, env.CookbookVersions[cookbook], next.RootMessages[rootCookbook], depsolverConstraintSources(next.Constraints[cookbook]))
}

func cloneDepsolverSolution(in depsolverSolution) depsolverSolution {
	out := depsolverSolution{
		Versions:     make(map[string]CookbookVersion, len(in.Versions)),
		Constraints:  make(map[string][]depsolverConstraintSource, len(in.Constraints)),
		RootMessages: make(map[string]string, len(in.RootMessages)),
	}
	for name, version := range in.Versions {
		out.Versions[name] = copyCookbookVersion(version)
	}
	for name, constraints := range in.Constraints {
		out.Constraints[name] = append([]depsolverConstraintSource(nil), constraints...)
	}
	for name, message := range in.RootMessages {
		out.RootMessages[name] = message
	}
	return out
}

func filterDepsolverCookbookRefs(refs []CookbookVersionRef, constraints []string) []CookbookVersionRef {
	if len(refs) == 0 || len(constraints) == 0 {
		return refs
	}

	out := make([]CookbookVersionRef, 0, len(refs))
	for _, ref := range refs {
		ok := true
		for _, constraint := range constraints {
			constraint = strings.TrimSpace(constraint)
			if constraint == "" {
				continue
			}
			if !cookbookConstraintMatches(ref.Version, constraint) {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, ref)
		}
	}
	return out
}

func depsolverVersionMatchesAll(version string, constraints []depsolverConstraintSource) bool {
	for _, constraint := range constraints {
		value := strings.TrimSpace(constraint.Constraint)
		if value == "" {
			continue
		}
		if !cookbookConstraintMatches(version, value) {
			return false
		}
	}
	return true
}

func depsolverConstraintValues(constraints []depsolverConstraintSource) []string {
	out := make([]string, 0, len(constraints))
	for _, constraint := range constraints {
		value := strings.TrimSpace(constraint.Constraint)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func depsolverConstraintSources(constraints []depsolverConstraintSource) []string {
	out := make([]string, 0, len(constraints))
	seen := make(map[string]struct{}, len(constraints))
	for _, constraint := range constraints {
		source := strings.TrimSpace(constraint.Source)
		if source == "" {
			continue
		}
		if _, ok := seen[source]; ok {
			continue
		}
		seen[source] = struct{}{}
		out = append(out, source)
	}
	sort.Strings(out)
	return out
}

func rootMissingCookbooksError(cookbooks []string) error {
	message := "Run list contains invalid items: no such cookbook " + cookbooks[0] + "."
	if len(cookbooks) > 1 {
		message = "Run list contains invalid items: no such cookbooks " + strings.Join(cookbooks, ", ") + "."
	}
	return &DepsolverError{
		Detail: map[string]any{
			"message":                    message,
			"non_existent_cookbooks":     append([]string(nil), cookbooks...),
			"cookbooks_with_no_versions": []string{},
		},
	}
}

func rootNoVersionsError(labels []string) error {
	target := labels[0]
	message := "Run list contains invalid items: no versions match the constraints on cookbook " + target + "."
	if len(labels) > 1 {
		message = "Run list contains invalid items: no versions match the constraints on cookbooks " + strings.Join(labels, ", ") + "."
	}
	return &DepsolverError{
		Detail: map[string]any{
			"message":                    message,
			"non_existent_cookbooks":     []string{},
			"cookbooks_with_no_versions": append([]string(nil), labels...),
		},
	}
}

func missingDependencyCookbookError(cookbook, rootMessage string, sources []string) error {
	return &DepsolverError{
		Detail: map[string]any{
			"message": fmt.Sprintf(
				"Unable to satisfy constraints on package %s, which does not exist, due to solution constraint %s. Solution constraints that may result in a constraint on %s: %s",
				cookbook,
				rootMessage,
				cookbook,
				formatDepsolverConstraintSources(sources),
			),
			"unsatisfiable_run_list_item": rootMessage,
			"non_existent_cookbooks":      []string{cookbook},
			"most_constrained_cookbooks":  []string{},
		},
	}
}

func unsatisfiedDependencyCookbookError(cookbook string, versions map[string]CookbookVersion, envConstraint, rootMessage string, sources []string) error {
	return &DepsolverError{
		Detail: map[string]any{
			"message": fmt.Sprintf(
				"Unable to satisfy constraints on package %s due to solution constraint %s. Solution constraints that may result in a constraint on %s: %s",
				cookbook,
				rootMessage,
				cookbook,
				formatDepsolverConstraintSources(sources),
			),
			"unsatisfiable_run_list_item": rootMessage,
			"non_existent_cookbooks":      []string{},
			"most_constrained_cookbooks":  []string{renderMostConstrainedCookbook(cookbook, versions, envConstraint)},
		},
	}
}

func renderMostConstrainedCookbook(cookbook string, versions map[string]CookbookVersion, envConstraint string) string {
	refs := filterEnvironmentCookbookRefs(cookbookVersionRefs(versions), envConstraint)
	if len(refs) == 0 {
		return cookbook + " -> []"
	}

	version := versions[refs[0].Version]
	dependencies := cookbookMetadataDependencies(version.Metadata)
	if len(dependencies) == 0 {
		return fmt.Sprintf("%s = %s -> []", cookbook, version.Version)
	}

	parts := make([]string, 0, len(dependencies))
	names := make([]string, 0, len(dependencies))
	for name := range dependencies {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		constraint := strings.TrimSpace(dependencies[name])
		if constraint == "" {
			constraint = ">= 0.0.0"
		}
		parts = append(parts, fmt.Sprintf("(%s %s)", name, constraint))
	}
	return fmt.Sprintf("%s = %s -> [%s]", cookbook, version.Version, strings.Join(parts, ", "))
}

func formatDepsolverConstraintSources(sources []string) string {
	if len(sources) == 0 {
		return "[]"
	}
	return "[" + strings.Join(sources, ", ") + "]"
}
