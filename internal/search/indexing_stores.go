package search

import (
	"context"
	"reflect"
	"sort"
	"sync"

	"github.com/oberones/OpenCook/internal/bootstrap"
)

type DocumentRef struct {
	Organization string
	Index        string
	Name         string
}

type DocumentIndexer interface {
	UpsertDocuments(context.Context, []Document) error
	DeleteDocuments(context.Context, []DocumentRef) error
}

func NewIndexingBootstrapCoreStore(delegate bootstrap.BootstrapCoreStore, indexer DocumentIndexer) bootstrap.BootstrapCoreStore {
	if delegate == nil {
		delegate = bootstrap.NewMemoryBootstrapCoreStore(bootstrap.BootstrapCoreState{})
	}
	return &indexingBootstrapCoreStore{
		delegate: delegate,
		indexer:  indexer,
	}
}

func NewIndexingCoreObjectStore(delegate bootstrap.CoreObjectStore, indexer DocumentIndexer) bootstrap.CoreObjectStore {
	if delegate == nil {
		delegate = bootstrap.NewMemoryCoreObjectStore(bootstrap.CoreObjectState{})
	}
	return &indexingCoreObjectStore{
		delegate: delegate,
		indexer:  indexer,
	}
}

type indexingBootstrapCoreStore struct {
	mu          sync.Mutex
	delegate    bootstrap.BootstrapCoreStore
	indexer     DocumentIndexer
	previous    bootstrap.BootstrapCoreState
	initialized bool
}

func (s *indexingBootstrapCoreStore) LoadBootstrapCore() (bootstrap.BootstrapCoreState, error) {
	// Loading is intentionally side-effect free: repair reloads may inspect
	// persisted state and then roll back live maps if verifier hydration fails.
	return s.delegate.LoadBootstrapCore()
}

func (s *indexingBootstrapCoreStore) SaveBootstrapCore(next bootstrap.BootstrapCoreState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	previous, err := s.previousLocked()
	if err != nil {
		return err
	}
	if err := s.delegate.SaveBootstrapCore(next); err != nil {
		return err
	}

	s.emitDiff(previous, next)
	s.previous = bootstrap.CloneBootstrapCoreState(next)
	s.initialized = true
	return nil
}

func (s *indexingBootstrapCoreStore) previousLocked() (bootstrap.BootstrapCoreState, error) {
	if s.initialized {
		return bootstrap.CloneBootstrapCoreState(s.previous), nil
	}
	state, err := s.delegate.LoadBootstrapCore()
	if err != nil {
		return bootstrap.BootstrapCoreState{}, err
	}
	s.previous = bootstrap.CloneBootstrapCoreState(state)
	s.initialized = true
	return bootstrap.CloneBootstrapCoreState(state), nil
}

func (s *indexingBootstrapCoreStore) emitDiff(previous, next bootstrap.BootstrapCoreState) {
	if s.indexer == nil {
		return
	}

	builder := NewDocumentBuilder()
	var upserts []Document
	var deletes []DocumentRef
	for _, orgName := range sortedStringUnion(previous.Orgs, next.Orgs) {
		previousClients := previous.Orgs[orgName].Clients
		nextClients := next.Orgs[orgName].Clients
		for _, clientName := range sortedStringUnion(previousClients, nextClients) {
			previousClient, hadPrevious := previousClients[clientName]
			nextClient, hasNext := nextClients[clientName]
			switch {
			case hadPrevious && !hasNext:
				deletes = append(deletes, DocumentRef{Organization: orgName, Index: "client", Name: clientName})
			case hasNext && (!hadPrevious || !reflect.DeepEqual(previousClient, nextClient)):
				upserts = append(upserts, builder.Client(orgName, nextClient))
			}
		}
	}
	emitIndexEvents(s.indexer, upserts, deletes)
}

type indexingCoreObjectStore struct {
	mu          sync.Mutex
	delegate    bootstrap.CoreObjectStore
	indexer     DocumentIndexer
	previous    bootstrap.CoreObjectState
	initialized bool
}

func (s *indexingCoreObjectStore) LoadCoreObjects() (bootstrap.CoreObjectState, error) {
	// Loading is intentionally side-effect free: repair reloads may inspect
	// persisted state before deciding whether live maps can be safely replaced.
	return s.delegate.LoadCoreObjects()
}

func (s *indexingCoreObjectStore) SaveCoreObjects(next bootstrap.CoreObjectState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	previous, err := s.previousLocked()
	if err != nil {
		return err
	}
	if err := s.delegate.SaveCoreObjects(next); err != nil {
		return err
	}

	s.emitDiff(previous, next)
	s.previous = bootstrap.CloneCoreObjectState(next)
	s.initialized = true
	return nil
}

func (s *indexingCoreObjectStore) previousLocked() (bootstrap.CoreObjectState, error) {
	if s.initialized {
		return bootstrap.CloneCoreObjectState(s.previous), nil
	}
	state, err := s.delegate.LoadCoreObjects()
	if err != nil {
		return bootstrap.CoreObjectState{}, err
	}
	s.previous = bootstrap.CloneCoreObjectState(state)
	s.initialized = true
	return bootstrap.CloneCoreObjectState(state), nil
}

func (s *indexingCoreObjectStore) emitDiff(previous, next bootstrap.CoreObjectState) {
	if s.indexer == nil {
		return
	}

	builder := NewDocumentBuilder()
	var upserts []Document
	var deletes []DocumentRef
	for _, orgName := range sortedStringUnion(previous.Orgs, next.Orgs) {
		previousOrg := previous.Orgs[orgName]
		nextOrg := next.Orgs[orgName]

		for _, name := range sortedStringUnion(previousOrg.Environments, nextOrg.Environments) {
			previousEnv, hadPrevious := previousOrg.Environments[name]
			nextEnv, hasNext := nextOrg.Environments[name]
			switch {
			case hadPrevious && !hasNext:
				deletes = append(deletes, DocumentRef{Organization: orgName, Index: "environment", Name: name})
			case hasNext && (!hadPrevious || !reflect.DeepEqual(previousEnv, nextEnv)):
				upserts = append(upserts, builder.Environment(orgName, nextEnv))
			}
		}

		for _, name := range sortedStringUnion(previousOrg.Nodes, nextOrg.Nodes) {
			previousNode, hadPrevious := previousOrg.Nodes[name]
			nextNode, hasNext := nextOrg.Nodes[name]
			switch {
			case hadPrevious && !hasNext:
				deletes = append(deletes, DocumentRef{Organization: orgName, Index: "node", Name: name})
			case hasNext && (!hadPrevious || !reflect.DeepEqual(previousNode, nextNode)):
				upserts = append(upserts, builder.Node(orgName, nextNode))
			}
		}

		for _, name := range sortedStringUnion(previousOrg.Roles, nextOrg.Roles) {
			previousRole, hadPrevious := previousOrg.Roles[name]
			nextRole, hasNext := nextOrg.Roles[name]
			switch {
			case hadPrevious && !hasNext:
				deletes = append(deletes, DocumentRef{Organization: orgName, Index: "role", Name: name})
			case hasNext && (!hadPrevious || !reflect.DeepEqual(previousRole, nextRole)):
				upserts = append(upserts, builder.Role(orgName, nextRole))
			}
		}

		for _, bagName := range sortedStringUnion(previousOrg.DataBagItems, nextOrg.DataBagItems) {
			previousItems := previousOrg.DataBagItems[bagName]
			nextItems := nextOrg.DataBagItems[bagName]
			for _, itemID := range sortedStringUnion(previousItems, nextItems) {
				previousItem, hadPrevious := previousItems[itemID]
				nextItem, hasNext := nextItems[itemID]
				switch {
				case hadPrevious && !hasNext:
					deletes = append(deletes, DocumentRef{Organization: orgName, Index: bagName, Name: itemID})
				case hasNext && (!hadPrevious || !reflect.DeepEqual(previousItem, nextItem)):
					upserts = append(upserts, builder.DataBagItem(orgName, bagName, nextItem))
				}
			}
		}
	}
	emitIndexEvents(s.indexer, upserts, deletes)
}

func emitIndexEvents(indexer DocumentIndexer, upserts []Document, deletes []DocumentRef) {
	ctx := context.Background()
	if len(deletes) > 0 {
		_ = indexer.DeleteDocuments(ctx, deletes)
	}
	if len(upserts) > 0 {
		_ = indexer.UpsertDocuments(ctx, upserts)
	}
}

func sortedStringUnion[V any](left, right map[string]V) []string {
	seen := make(map[string]struct{}, len(left)+len(right))
	for key := range left {
		seen[key] = struct{}{}
	}
	for key := range right {
		seen[key] = struct{}{}
	}

	out := make([]string, 0, len(seen))
	for key := range seen {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func (c *OpenSearchClient) UpsertDocuments(ctx context.Context, docs []Document) error {
	if len(docs) == 0 {
		return nil
	}
	if err := c.BulkUpsert(ctx, docs); err != nil {
		return err
	}
	return c.Refresh(ctx)
}

func (c *OpenSearchClient) DeleteDocuments(ctx context.Context, refs []DocumentRef) error {
	if len(refs) == 0 {
		return nil
	}
	for _, ref := range refs {
		if err := c.DeleteDocument(ctx, OpenSearchDocumentIDForRef(ref)); err != nil {
			return err
		}
	}
	return c.Refresh(ctx)
}

func OpenSearchDocumentIDForRef(ref DocumentRef) string {
	return ref.Organization + "/" + ref.Index + "/" + ref.Name
}
