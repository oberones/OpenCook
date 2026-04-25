package search

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/bootstrap"
)

var errIndexingStoreFailed = errors.New("indexing store failed")

func TestIndexingStoresEmitSuccessfulMutationEvents(t *testing.T) {
	recorder := &recordingDocumentIndexer{}
	service := newIndexingStoreTestService(recorder, bootstrap.NewMemoryBootstrapCoreStore(bootstrap.BootstrapCoreState{}), bootstrap.NewMemoryCoreObjectStore(bootstrap.CoreObjectState{}))
	creator := authn.Principal{Type: "user", Name: "pivotal"}

	if _, _, _, err := service.CreateOrganization(bootstrap.CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "pivotal",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}
	recorder.requireUpsert(t, "client", "ponyville-validator")

	if _, _, err := service.CreateClient("ponyville", bootstrap.CreateClientInput{Name: "twilight"}); err != nil {
		t.Fatalf("CreateClient() error = %v", err)
	}
	recorder.requireUpsert(t, "client", "twilight")
	if _, err := service.DeleteClient("ponyville", "twilight"); err != nil {
		t.Fatalf("DeleteClient() error = %v", err)
	}
	recorder.requireDelete(t, "client", "twilight")

	if _, err := service.CreateEnvironment("ponyville", bootstrap.CreateEnvironmentInput{
		Creator: creator,
		Payload: map[string]any{
			"name": "production",
		},
	}); err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	recorder.requireUpsert(t, "environment", "production")
	if _, err := service.UpdateEnvironment("ponyville", "production", bootstrap.UpdateEnvironmentInput{
		Payload: map[string]any{
			"name":        "prod2",
			"description": "Production",
		},
	}); err != nil {
		t.Fatalf("UpdateEnvironment() error = %v", err)
	}
	recorder.requireDelete(t, "environment", "production")
	recorder.requireUpsert(t, "environment", "prod2")
	if _, err := service.DeleteEnvironment("ponyville", "prod2"); err != nil {
		t.Fatalf("DeleteEnvironment() error = %v", err)
	}
	recorder.requireDelete(t, "environment", "prod2")

	if _, err := service.CreateNode("ponyville", bootstrap.CreateNodeInput{
		Creator: creator,
		Payload: map[string]any{
			"name":     "node1",
			"run_list": []any{"base"},
		},
	}); err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	recorder.requireUpsert(t, "node", "node1")
	if _, err := service.UpdateNode("ponyville", "node1", bootstrap.UpdateNodeInput{
		Payload: map[string]any{
			"name":     "node1",
			"run_list": []any{"base", "role[web]"},
		},
	}); err != nil {
		t.Fatalf("UpdateNode() error = %v", err)
	}
	recorder.requireUpsert(t, "node", "node1")
	if _, err := service.DeleteNode("ponyville", "node1"); err != nil {
		t.Fatalf("DeleteNode() error = %v", err)
	}
	recorder.requireDelete(t, "node", "node1")

	if _, err := service.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Creator: creator,
		Payload: map[string]any{
			"name":     "web",
			"run_list": []any{"base"},
		},
	}); err != nil {
		t.Fatalf("CreateRole() error = %v", err)
	}
	recorder.requireUpsert(t, "role", "web")
	if _, err := service.UpdateRole("ponyville", "web", bootstrap.UpdateRoleInput{
		Payload: map[string]any{
			"name":        "web",
			"description": "Web",
		},
	}); err != nil {
		t.Fatalf("UpdateRole() error = %v", err)
	}
	recorder.requireUpsert(t, "role", "web")
	if _, err := service.DeleteRole("ponyville", "web"); err != nil {
		t.Fatalf("DeleteRole() error = %v", err)
	}
	recorder.requireDelete(t, "role", "web")

	if _, err := service.CreateDataBag("ponyville", bootstrap.CreateDataBagInput{
		Creator: creator,
		Payload: map[string]any{"name": "ponies"},
	}); err != nil {
		t.Fatalf("CreateDataBag() error = %v", err)
	}
	if _, err := service.CreateDataBagItem("ponyville", "ponies", bootstrap.CreateDataBagItemInput{
		Payload: map[string]any{"id": "twilight", "skill": "magic"},
	}); err != nil {
		t.Fatalf("CreateDataBagItem() error = %v", err)
	}
	recorder.requireUpsert(t, "ponies", "twilight")
	if _, err := service.UpdateDataBagItem("ponyville", "ponies", "twilight", bootstrap.UpdateDataBagItemInput{
		Payload: map[string]any{"id": "twilight", "skill": "friendship"},
	}); err != nil {
		t.Fatalf("UpdateDataBagItem() error = %v", err)
	}
	recorder.requireUpsert(t, "ponies", "twilight")
	if _, err := service.DeleteDataBagItem("ponyville", "ponies", "twilight"); err != nil {
		t.Fatalf("DeleteDataBagItem() error = %v", err)
	}
	recorder.requireDelete(t, "ponies", "twilight")
}

func TestIndexingStoresDoNotEmitForInvalidWrites(t *testing.T) {
	recorder := &recordingDocumentIndexer{}
	service := newIndexingStoreTestService(recorder, bootstrap.NewMemoryBootstrapCoreStore(bootstrap.BootstrapCoreState{}), bootstrap.NewMemoryCoreObjectStore(bootstrap.CoreObjectState{}))
	if _, _, _, err := service.CreateOrganization(bootstrap.CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "pivotal",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

	before := recorder.snapshot()
	if _, err := service.CreateNode("ponyville", bootstrap.CreateNodeInput{
		Creator: authn.Principal{Type: "user", Name: "pivotal"},
		Payload: map[string]any{"run_list": []any{"base"}},
	}); err == nil {
		t.Fatal("CreateNode(invalid) error = nil, want validation failure")
	}
	recorder.requireSnapshot(t, before)

	if _, err := service.CreateDataBagItem("ponyville", "missing", bootstrap.CreateDataBagItemInput{
		Payload: map[string]any{"id": "twilight"},
	}); err == nil {
		t.Fatal("CreateDataBagItem(missing bag) error = nil, want failure")
	}
	recorder.requireSnapshot(t, before)
}

func TestIndexingStoresDoNotEmitForFailedPersistenceWrites(t *testing.T) {
	recorder := &recordingDocumentIndexer{}
	service := newIndexingStoreTestService(recorder, failingBootstrapCoreStore{}, bootstrap.NewMemoryCoreObjectStore(bootstrap.CoreObjectState{}))
	if _, _, _, err := service.CreateOrganization(bootstrap.CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "pivotal",
	}); !errors.Is(err, errIndexingStoreFailed) {
		t.Fatalf("CreateOrganization() error = %v, want persistence failure", err)
	}
	if len(recorder.upserts) != 0 || len(recorder.deletes) != 0 {
		t.Fatalf("recorder emitted events for failed bootstrap write: upserts=%v deletes=%v", recorder.upserts, recorder.deletes)
	}

	recorder = &recordingDocumentIndexer{}
	service = newIndexingStoreTestService(recorder, bootstrap.NewMemoryBootstrapCoreStore(bootstrap.BootstrapCoreState{}), failingCoreObjectStore{})
	if _, _, _, err := service.CreateOrganization(bootstrap.CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "pivotal",
	}); err != nil {
		t.Fatalf("CreateOrganization() setup error = %v", err)
	}
	before := recorder.snapshot()
	if _, err := service.CreateNode("ponyville", bootstrap.CreateNodeInput{
		Creator: authn.Principal{Type: "user", Name: "pivotal"},
		Payload: map[string]any{"name": "node1"},
	}); !errors.Is(err, errIndexingStoreFailed) {
		t.Fatalf("CreateNode() error = %v, want persistence failure", err)
	}
	recorder.requireSnapshot(t, before)
}

func newIndexingStoreTestService(recorder *recordingDocumentIndexer, bootstrapCore bootstrap.BootstrapCoreStore, coreObjects bootstrap.CoreObjectStore) *bootstrap.Service {
	return bootstrap.NewService(authn.NewMemoryKeyStore(), bootstrap.Options{
		SuperuserName: "pivotal",
		BootstrapCoreStoreFactory: func(*bootstrap.Service) bootstrap.BootstrapCoreStore {
			return NewIndexingBootstrapCoreStore(bootstrapCore, recorder)
		},
		CoreObjectStoreFactory: func(*bootstrap.Service) bootstrap.CoreObjectStore {
			return NewIndexingCoreObjectStore(coreObjects, recorder)
		},
	})
}

type recordingDocumentIndexer struct {
	upserts []DocumentRef
	deletes []DocumentRef
}

type indexingRecorderSnapshot struct {
	upserts []DocumentRef
	deletes []DocumentRef
}

func (r *recordingDocumentIndexer) UpsertDocuments(_ context.Context, docs []Document) error {
	for _, doc := range docs {
		r.upserts = append(r.upserts, DocumentRef{
			Organization: doc.Resource.Organization,
			Index:        doc.Index,
			Name:         doc.Name,
		})
	}
	return nil
}

func (r *recordingDocumentIndexer) DeleteDocuments(_ context.Context, refs []DocumentRef) error {
	r.deletes = append(r.deletes, refs...)
	return nil
}

func (r *recordingDocumentIndexer) snapshot() indexingRecorderSnapshot {
	return indexingRecorderSnapshot{
		upserts: append([]DocumentRef(nil), r.upserts...),
		deletes: append([]DocumentRef(nil), r.deletes...),
	}
}

func (r *recordingDocumentIndexer) requireSnapshot(t *testing.T, want indexingRecorderSnapshot) {
	t.Helper()

	if !reflect.DeepEqual(r.upserts, want.upserts) || !reflect.DeepEqual(r.deletes, want.deletes) {
		t.Fatalf("recorder changed: upserts=%v deletes=%v, want upserts=%v deletes=%v", r.upserts, r.deletes, want.upserts, want.deletes)
	}
}

func (r *recordingDocumentIndexer) requireUpsert(t *testing.T, index, name string) {
	t.Helper()

	requireRef(t, r.upserts, index, name)
}

func (r *recordingDocumentIndexer) requireDelete(t *testing.T, index, name string) {
	t.Helper()

	requireRef(t, r.deletes, index, name)
}

func requireRef(t *testing.T, refs []DocumentRef, index, name string) {
	t.Helper()

	for _, ref := range refs {
		if ref.Organization == "ponyville" && ref.Index == index && ref.Name == name {
			return
		}
	}
	t.Fatalf("refs = %v, want ponyville/%s/%s", refs, index, name)
}

type failingBootstrapCoreStore struct{}

func (failingBootstrapCoreStore) LoadBootstrapCore() (bootstrap.BootstrapCoreState, error) {
	return bootstrap.BootstrapCoreState{}, nil
}

func (failingBootstrapCoreStore) SaveBootstrapCore(bootstrap.BootstrapCoreState) error {
	return errIndexingStoreFailed
}

type failingCoreObjectStore struct{}

func (failingCoreObjectStore) LoadCoreObjects() (bootstrap.CoreObjectState, error) {
	return bootstrap.CoreObjectState{}, nil
}

func (failingCoreObjectStore) SaveCoreObjects(bootstrap.CoreObjectState) error {
	return errIndexingStoreFailed
}
