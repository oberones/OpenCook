package search

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/oberones/OpenCook/internal/bootstrap"
)

type ConsistencyPlan struct {
	AllOrganizations bool
	Organization     string
	Index            string
	Repair           bool
	DryRun           bool
}

type ConsistencyCounts struct {
	Expected    int `json:"expected"`
	Observed    int `json:"observed"`
	Missing     int `json:"missing"`
	Stale       int `json:"stale"`
	Unsupported int `json:"unsupported"`
	Upserted    int `json:"upserted"`
	Deleted     int `json:"deleted"`
	Skipped     int `json:"skipped"`
	Failed      int `json:"failed"`
	Clean       int `json:"clean"`
}

type ConsistencyObjectCount struct {
	Organization string `json:"organization"`
	Index        string `json:"index"`
	Expected     int    `json:"expected"`
	Observed     int    `json:"observed"`
	Missing      int    `json:"missing"`
	Stale        int    `json:"stale"`
}

type ConsistencyResult struct {
	AllOrganizations  bool                     `json:"all_organizations"`
	Organization      string                   `json:"organization,omitempty"`
	Index             string                   `json:"index,omitempty"`
	Repair            bool                     `json:"repair,omitempty"`
	DryRun            bool                     `json:"dry_run,omitempty"`
	Counts            ConsistencyCounts        `json:"counts"`
	ObjectCounts      []ConsistencyObjectCount `json:"object_counts,omitempty"`
	MissingDocuments  []string                 `json:"missing_documents,omitempty"`
	StaleDocuments    []string                 `json:"stale_documents,omitempty"`
	UnsupportedScopes []string                 `json:"unsupported_scopes,omitempty"`
	Failures          []string                 `json:"failures,omitempty"`
	Duration          time.Duration            `json:"duration"`
}

type ConsistencyTarget interface {
	Ping(context.Context) error
	EnsureChefIndex(context.Context) error
	SearchIDs(context.Context, Query) ([]string, error)
	BulkUpsert(context.Context, []Document) error
	DeleteDocument(context.Context, string) error
	Refresh(context.Context) error
}

type ConsistencyService struct {
	state  *bootstrap.Service
	target ConsistencyTarget
	now    func() time.Time
}

func NewConsistencyService(state *bootstrap.Service, target ConsistencyTarget) *ConsistencyService {
	return &ConsistencyService{
		state:  state,
		target: target,
		now:    time.Now,
	}
}

func (s *ConsistencyService) WithNow(now func() time.Time) *ConsistencyService {
	if now != nil {
		s.now = now
	}
	return s
}

func (s *ConsistencyService) Run(ctx context.Context, plan ConsistencyPlan) (result ConsistencyResult, err error) {
	if s == nil || s.state == nil {
		return ConsistencyResult{}, fmt.Errorf("%w: bootstrap state is required", ErrInvalidConfiguration)
	}
	if s.target == nil {
		return ConsistencyResult{}, fmt.Errorf("%w: opensearch target is required", ErrInvalidConfiguration)
	}
	if s.now == nil {
		s.now = time.Now
	}

	start := s.now()
	plan = normalizeConsistencyPlan(plan)
	result = ConsistencyResult{
		AllOrganizations: plan.AllOrganizations,
		Organization:     plan.Organization,
		Index:            plan.Index,
		Repair:           plan.Repair,
		DryRun:           plan.DryRun,
	}
	defer func() {
		result.Duration = s.now().Sub(start)
	}()

	docs, err := documentsFromBootstrapState(s.state)
	if err != nil {
		return markConsistencyFailure(result, err)
	}
	expectedDocs, err := filterConsistencyDocuments(s.state, docs, plan)
	if err != nil {
		return markConsistencyFailure(result, err)
	}

	if err := s.target.Ping(ctx); err != nil {
		return markConsistencyFailure(result, err)
	}
	if plan.Repair && !plan.DryRun {
		if err := s.target.EnsureChefIndex(ctx); err != nil {
			return markConsistencyFailure(result, err)
		}
	}

	observedIDs, err := s.target.SearchIDs(ctx, Query{
		Organization: plan.Organization,
		Index:        plan.Index,
		Q:            "*:*",
	})
	if err != nil {
		return markConsistencyFailure(result, err)
	}

	analysis := analyzeSearchConsistency(s.state, expectedDocs, observedIDs)
	result.Counts = analysis.counts
	result.ObjectCounts = analysis.sortedObjectCounts()
	result.MissingDocuments = analysis.missingIDs
	result.StaleDocuments = analysis.staleIDs
	result.UnsupportedScopes = analysis.unsupportedScopes
	if result.Counts.Missing == 0 && result.Counts.Stale == 0 && result.Counts.Unsupported == 0 {
		result.Counts.Clean = 1
	}

	if plan.Repair {
		if plan.DryRun {
			result.Counts.Skipped = result.Counts.Missing + result.Counts.Stale
			return result, nil
		}
		if err := s.repair(ctx, analysis); err != nil {
			result.Counts = analysis.counts
			return markConsistencyFailure(result, err)
		}
		result.Counts.Upserted = len(analysis.missingDocs)
		result.Counts.Deleted = len(analysis.staleIDs)
	}

	return result, nil
}

func (s *ConsistencyService) repair(ctx context.Context, analysis consistencyAnalysis) error {
	if len(analysis.missingDocs) > 0 {
		if err := s.target.BulkUpsert(ctx, analysis.missingDocs); err != nil {
			return err
		}
	}
	for _, id := range analysis.staleIDs {
		if err := s.target.DeleteDocument(ctx, id); err != nil {
			return err
		}
	}
	if len(analysis.missingDocs) > 0 || len(analysis.staleIDs) > 0 {
		if err := s.target.Refresh(ctx); err != nil {
			return err
		}
	}
	return nil
}

func normalizeConsistencyPlan(plan ConsistencyPlan) ConsistencyPlan {
	plan.Organization = strings.TrimSpace(plan.Organization)
	plan.Index = strings.TrimSpace(plan.Index)
	if plan.Organization == "" {
		plan.AllOrganizations = true
	}
	return plan
}

func filterConsistencyDocuments(state *bootstrap.Service, docs []Document, plan ConsistencyPlan) ([]Document, error) {
	if err := validateConsistencyPlan(state, plan); err != nil {
		return nil, err
	}
	filtered := make([]Document, 0, len(docs))
	for _, doc := range docs {
		if plan.Organization != "" && doc.Resource.Organization != plan.Organization {
			continue
		}
		if plan.Index != "" && doc.Index != plan.Index {
			continue
		}
		filtered = append(filtered, doc)
	}
	return filtered, nil
}

func validateConsistencyPlan(state *bootstrap.Service, plan ConsistencyPlan) error {
	if state == nil {
		return ErrUnavailable
	}
	if plan.AllOrganizations && plan.Organization != "" {
		return fmt.Errorf("%w: all organizations cannot be combined with a named organization", ErrInvalidConfiguration)
	}
	if plan.Organization != "" {
		if _, ok := state.GetOrganization(plan.Organization); !ok {
			return ErrOrganizationNotFound
		}
		if plan.Index != "" && !consistencyIndexSupportedForOrg(state, plan.Organization, plan.Index) {
			return ErrIndexNotFound
		}
		return nil
	}
	if plan.Index != "" && !consistencyIndexSupportedForAnyOrg(state, plan.Index) {
		return ErrIndexNotFound
	}
	return nil
}

func consistencyIndexSupportedForAnyOrg(state *bootstrap.Service, index string) bool {
	if consistencyBuiltInIndex(index) {
		return true
	}
	for orgName := range state.ListOrganizations() {
		if consistencyIndexSupportedForOrg(state, orgName, index) {
			return true
		}
	}
	return false
}

func consistencyIndexSupportedForOrg(state *bootstrap.Service, org, index string) bool {
	if consistencyBuiltInIndex(index) {
		return true
	}
	_, orgExists, bagExists := state.GetDataBag(org, index)
	return orgExists && bagExists
}

func consistencyBuiltInIndex(index string) bool {
	for _, builtIn := range builtInIndexes {
		if index == builtIn {
			return true
		}
	}
	return false
}

type consistencyAnalysis struct {
	counts            ConsistencyCounts
	objectCounts      map[string]*ConsistencyObjectCount
	missingIDs        []string
	missingDocs       []Document
	staleIDs          []string
	unsupportedScopes []string
}

func analyzeSearchConsistency(state *bootstrap.Service, expectedDocs []Document, observedIDs []string) consistencyAnalysis {
	analysis := consistencyAnalysis{
		objectCounts: make(map[string]*ConsistencyObjectCount),
	}
	expectedByID := make(map[string]Document, len(expectedDocs))
	for _, doc := range expectedDocs {
		id := OpenSearchDocumentID(doc)
		expectedByID[id] = doc
		count := analysis.countForRef(DocumentRef{
			Organization: doc.Resource.Organization,
			Index:        doc.Index,
			Name:         doc.Name,
		})
		count.Expected++
	}
	analysis.counts.Expected = len(expectedDocs)

	observed := make(map[string]struct{}, len(observedIDs))
	unsupported := map[string]struct{}{}
	for _, id := range observedIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		observed[id] = struct{}{}
		analysis.counts.Observed++
		ref, ok := parseOpenSearchDocumentID(id)
		unsupportedScope := false
		if ok {
			var scope string
			scope, unsupportedScope = consistencyUnsupportedScope(state, ref)
			if unsupportedScope {
				unsupported[scope] = struct{}{}
			} else {
				analysis.countForRef(ref).Observed++
			}
		} else {
			unsupported["malformed-document-id"] = struct{}{}
		}
		if _, expected := expectedByID[id]; !expected {
			analysis.staleIDs = append(analysis.staleIDs, id)
			if ok && !unsupportedScope {
				analysis.countForRef(ref).Stale++
			}
		}
	}

	for id, doc := range expectedByID {
		if _, ok := observed[id]; ok {
			continue
		}
		analysis.missingIDs = append(analysis.missingIDs, id)
		analysis.missingDocs = append(analysis.missingDocs, doc)
		analysis.countForRef(DocumentRef{
			Organization: doc.Resource.Organization,
			Index:        doc.Index,
			Name:         doc.Name,
		}).Missing++
	}

	sort.Strings(analysis.missingIDs)
	sort.Strings(analysis.staleIDs)
	sort.Slice(analysis.missingDocs, func(i, j int) bool {
		return OpenSearchDocumentID(analysis.missingDocs[i]) < OpenSearchDocumentID(analysis.missingDocs[j])
	})
	for scope := range unsupported {
		analysis.unsupportedScopes = append(analysis.unsupportedScopes, scope)
	}
	sort.Strings(analysis.unsupportedScopes)
	analysis.counts.Missing = len(analysis.missingIDs)
	analysis.counts.Stale = len(analysis.staleIDs)
	analysis.counts.Unsupported = len(analysis.unsupportedScopes)
	return analysis
}

func (a *consistencyAnalysis) countForRef(ref DocumentRef) *ConsistencyObjectCount {
	key := ref.Organization + "\x00" + ref.Index
	count := a.objectCounts[key]
	if count == nil {
		count = &ConsistencyObjectCount{
			Organization: ref.Organization,
			Index:        ref.Index,
		}
		a.objectCounts[key] = count
	}
	return count
}

func (a consistencyAnalysis) sortedObjectCounts() []ConsistencyObjectCount {
	out := make([]ConsistencyObjectCount, 0, len(a.objectCounts))
	for _, count := range a.objectCounts {
		out = append(out, *count)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Organization == out[j].Organization {
			return out[i].Index < out[j].Index
		}
		return out[i].Organization < out[j].Organization
	})
	return out
}

func consistencyUnsupportedScope(state *bootstrap.Service, ref DocumentRef) (string, bool) {
	if _, ok := state.GetOrganization(ref.Organization); !ok {
		return ref.Organization + "/" + ref.Index, true
	}
	if !consistencyIndexSupportedForOrg(state, ref.Organization, ref.Index) {
		return ref.Organization + "/" + ref.Index, true
	}
	return "", false
}

func markConsistencyFailure(result ConsistencyResult, err error) (ConsistencyResult, error) {
	result.Counts.Failed++
	result.Failures = append(result.Failures, reindexFailureSummary(err))
	return result, err
}
