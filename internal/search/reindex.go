package search

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/oberones/OpenCook/internal/bootstrap"
)

type ReindexMode string

const (
	ReindexModeDrop     ReindexMode = "drop"
	ReindexModeReindex  ReindexMode = "reindex"
	ReindexModeComplete ReindexMode = "complete"
)

type ReindexPlan struct {
	Mode             ReindexMode
	AllOrganizations bool
	Organization     string
	Index            string
	Names            []string
	DryRun           bool
}

type ReindexCounts struct {
	Scanned  int `json:"scanned"`
	Upserted int `json:"upserted"`
	Deleted  int `json:"deleted"`
	Skipped  int `json:"skipped"`
	Missing  int `json:"missing"`
	Failed   int `json:"failed"`
}

type ReindexResult struct {
	Mode             ReindexMode   `json:"mode"`
	AllOrganizations bool          `json:"all_organizations"`
	Organization     string        `json:"organization,omitempty"`
	Index            string        `json:"index,omitempty"`
	Names            []string      `json:"names,omitempty"`
	DryRun           bool          `json:"dry_run,omitempty"`
	Counts           ReindexCounts `json:"counts"`
	Failures         []string      `json:"failures,omitempty"`
	Duration         time.Duration `json:"duration"`
}

type ReindexTarget interface {
	Ping(context.Context) error
	EnsureChefIndex(context.Context) error
	DeleteByQuery(context.Context, string, string) error
	DeleteDocument(context.Context, string) error
	BulkUpsert(context.Context, []Document) error
	Refresh(context.Context) error
}

type ReindexService struct {
	state  *bootstrap.Service
	target ReindexTarget
	now    func() time.Time
}

func NewReindexService(state *bootstrap.Service, target ReindexTarget) *ReindexService {
	return &ReindexService{
		state:  state,
		target: target,
		now:    time.Now,
	}
}

func (s *ReindexService) WithNow(now func() time.Time) *ReindexService {
	if now != nil {
		s.now = now
	}
	return s
}

func (s *ReindexService) Run(ctx context.Context, plan ReindexPlan) (result ReindexResult, err error) {
	if s == nil || s.state == nil {
		return ReindexResult{}, fmt.Errorf("%w: bootstrap state is required", ErrInvalidConfiguration)
	}
	if s.target == nil {
		return ReindexResult{}, fmt.Errorf("%w: opensearch target is required", ErrInvalidConfiguration)
	}
	if s.now == nil {
		s.now = time.Now
	}

	start := s.now()
	plan = normalizeReindexPlan(plan)
	result = ReindexResult{
		Mode:             plan.Mode,
		AllOrganizations: plan.AllOrganizations,
		Organization:     plan.Organization,
		Index:            plan.Index,
		Names:            append([]string(nil), plan.Names...),
		DryRun:           plan.DryRun,
	}
	defer func() {
		result.Duration = s.now().Sub(start)
	}()

	docs, err := documentsFromBootstrapState(s.state)
	if err != nil {
		return markReindexFailure(result, err)
	}
	filtered, missing, err := filterReindexDocuments(s.state, docs, plan)
	if err != nil {
		return markReindexFailure(result, err)
	}
	result.Counts.Scanned = len(filtered)
	result.Counts.Missing = missing

	switch plan.Mode {
	case ReindexModeDrop, ReindexModeReindex, ReindexModeComplete:
	default:
		return markReindexFailure(result, fmt.Errorf("%w: unsupported reindex mode %q", ErrInvalidConfiguration, plan.Mode))
	}

	if plan.DryRun {
		result.Counts.Skipped = skippedReindexCount(plan, filtered)
		return result, nil
	}

	if err := s.target.Ping(ctx); err != nil {
		return markReindexFailure(result, err)
	}
	if err := s.target.EnsureChefIndex(ctx); err != nil {
		return markReindexFailure(result, err)
	}

	if plan.Mode == ReindexModeDrop || plan.Mode == ReindexModeComplete {
		deleted, err := s.drop(ctx, plan, filtered)
		if err != nil {
			return markReindexFailure(result, err)
		}
		result.Counts.Deleted = deleted
	}

	if (plan.Mode == ReindexModeReindex || plan.Mode == ReindexModeComplete) && len(filtered) > 0 {
		if err := s.target.BulkUpsert(ctx, filtered); err != nil {
			return markReindexFailure(result, err)
		}
		result.Counts.Upserted = len(filtered)
	}

	if err := s.target.Refresh(ctx); err != nil {
		return markReindexFailure(result, err)
	}
	return result, nil
}

func (s *ReindexService) drop(ctx context.Context, plan ReindexPlan, docs []Document) (int, error) {
	if len(plan.Names) == 0 {
		if err := s.target.DeleteByQuery(ctx, deleteByQueryOrg(plan), plan.Index); err != nil {
			return 0, err
		}
		return len(docs), nil
	}

	for _, name := range plan.Names {
		ref := DocumentRef{
			Organization: plan.Organization,
			Index:        plan.Index,
			Name:         name,
		}
		if err := s.target.DeleteDocument(ctx, OpenSearchDocumentIDForRef(ref)); err != nil {
			return 0, err
		}
	}
	return len(plan.Names), nil
}

func normalizeReindexPlan(plan ReindexPlan) ReindexPlan {
	plan.Organization = strings.TrimSpace(plan.Organization)
	plan.Index = strings.TrimSpace(plan.Index)
	if plan.Mode == "" {
		plan.Mode = ReindexModeComplete
	}
	seen := make(map[string]struct{}, len(plan.Names))
	names := make([]string, 0, len(plan.Names))
	for _, name := range plan.Names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	sort.Strings(names)
	plan.Names = names
	return plan
}

func filterReindexDocuments(state *bootstrap.Service, docs []Document, plan ReindexPlan) ([]Document, int, error) {
	if err := validateReindexPlan(state, plan); err != nil {
		return nil, 0, err
	}

	names := make(map[string]struct{}, len(plan.Names))
	for _, name := range plan.Names {
		names[name] = struct{}{}
	}
	foundNames := make(map[string]struct{}, len(plan.Names))
	filtered := make([]Document, 0, len(docs))
	for _, doc := range docs {
		if plan.Organization != "" && doc.Resource.Organization != plan.Organization {
			continue
		}
		if plan.Index != "" && doc.Index != plan.Index {
			continue
		}
		if len(names) > 0 {
			if _, ok := names[doc.Name]; !ok {
				continue
			}
			foundNames[doc.Name] = struct{}{}
		}
		filtered = append(filtered, doc)
	}

	missing := 0
	for name := range names {
		if _, ok := foundNames[name]; !ok {
			missing++
		}
	}
	return filtered, missing, nil
}

func validateReindexPlan(state *bootstrap.Service, plan ReindexPlan) error {
	if state == nil {
		return ErrUnavailable
	}
	if plan.AllOrganizations && plan.Organization != "" {
		return fmt.Errorf("%w: all organizations cannot be combined with a named organization", ErrInvalidConfiguration)
	}
	if !plan.AllOrganizations && plan.Organization == "" {
		return fmt.Errorf("%w: organization or all organizations is required", ErrInvalidConfiguration)
	}
	if plan.AllOrganizations && plan.Index != "" {
		return fmt.Errorf("%w: index-scoped reindex requires one organization", ErrInvalidConfiguration)
	}
	if len(plan.Names) > 0 && (plan.Organization == "" || plan.Index == "") {
		return fmt.Errorf("%w: named-item reindex requires organization and index", ErrInvalidConfiguration)
	}
	if plan.Organization == "" {
		return nil
	}
	if _, ok := state.GetOrganization(plan.Organization); !ok {
		return ErrOrganizationNotFound
	}
	if plan.Index == "" {
		return nil
	}
	indexes, err := searchIndexesForState(state, plan.Organization)
	if err != nil {
		return err
	}
	for _, index := range indexes {
		if index == plan.Index {
			return nil
		}
	}
	return ErrIndexNotFound
}

func deleteByQueryOrg(plan ReindexPlan) string {
	if plan.AllOrganizations {
		return ""
	}
	return plan.Organization
}

func skippedReindexCount(plan ReindexPlan, docs []Document) int {
	switch plan.Mode {
	case ReindexModeDrop:
		if len(plan.Names) > 0 {
			return len(plan.Names)
		}
		return len(docs)
	case ReindexModeReindex:
		return len(docs)
	case ReindexModeComplete:
		if len(plan.Names) > 0 {
			return len(plan.Names) + len(docs)
		}
		return len(docs) * 2
	default:
		return 0
	}
}

func markReindexFailure(result ReindexResult, err error) (ReindexResult, error) {
	result.Counts.Failed++
	result.Failures = append(result.Failures, reindexFailureSummary(err))
	return result, err
}

func reindexFailureSummary(err error) string {
	switch {
	case err == nil:
		return ""
	case strings.Contains(err.Error(), ErrUnavailable.Error()):
		return ErrUnavailable.Error()
	case strings.Contains(err.Error(), ErrInvalidConfiguration.Error()):
		return ErrInvalidConfiguration.Error()
	case strings.Contains(err.Error(), ErrRejected.Error()):
		return ErrRejected.Error()
	case strings.Contains(err.Error(), ErrOrganizationNotFound.Error()):
		return ErrOrganizationNotFound.Error()
	case strings.Contains(err.Error(), ErrIndexNotFound.Error()):
		return ErrIndexNotFound.Error()
	default:
		return "reindex failed"
	}
}
