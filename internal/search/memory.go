package search

import (
	"context"
	"sort"
	"strings"

	"github.com/oberones/OpenCook/internal/bootstrap"
)

var builtInIndexes = []string{"client", "environment", "node", "role"}

type MemoryIndex struct {
	state  *bootstrap.Service
	target string
}

func NewMemoryIndex(state *bootstrap.Service, target string) MemoryIndex {
	return MemoryIndex{
		state:  state,
		target: strings.TrimSpace(target),
	}
}

func (i MemoryIndex) Name() string {
	return "memory-compat-search"
}

func (i MemoryIndex) Status() Status {
	message := "search compatibility routes are backed by in-memory state"
	if i.target != "" {
		message = "OPENCOOK_OPENSEARCH_URL is configured; memory search fallback remains active until OpenSearch indexing is activated"
	}

	return Status{
		Backend:    "memory-compat",
		Configured: i.state != nil,
		Message:    message,
	}
}

func (i MemoryIndex) Indexes(_ context.Context, org string) ([]string, error) {
	return searchIndexesForState(i.state, org)
}

// Search evaluates the shared compiled query plan against documents derived
// from the in-memory bootstrap state.
func (i MemoryIndex) Search(_ context.Context, query Query) (Result, error) {
	if i.state == nil {
		return Result{}, ErrUnavailable
	}
	if err := ensureSearchOrganization(i.state, query.Organization); err != nil {
		return Result{}, err
	}

	docs, err := i.documentsForQuery(query)
	if err != nil {
		return Result{}, err
	}

	plan := CompileQuery(query.Q)
	if err := plan.Err(); err != nil {
		return Result{}, err
	}
	matched := make([]Document, 0, len(docs))
	for _, doc := range docs {
		if !plan.Matches(doc) {
			continue
		}
		matched = append(matched, doc)
	}

	return Result{Documents: matched}, nil
}

func (i MemoryIndex) documentsForQuery(query Query) ([]Document, error) {
	switch strings.TrimSpace(query.Index) {
	case "client":
		return i.clientDocuments(query.Organization)
	case "environment":
		return i.environmentDocuments(query.Organization)
	case "node":
		return i.nodeDocuments(query.Organization)
	case "role":
		return i.roleDocuments(query.Organization)
	default:
		return i.dataBagDocuments(query.Organization, query.Index)
	}
}

func (i MemoryIndex) clientDocuments(org string) ([]Document, error) {
	clients, ok := i.state.ListClients(org)
	if !ok {
		return nil, ErrOrganizationNotFound
	}

	names := sortedKeys(clients)
	out := make([]Document, 0, len(names))
	builder := NewDocumentBuilder()
	for _, name := range names {
		client, exists := i.state.GetClient(org, name)
		if !exists {
			continue
		}
		out = append(out, builder.Client(org, client))
	}
	return out, nil
}

func (i MemoryIndex) environmentDocuments(org string) ([]Document, error) {
	environments, ok := i.state.ListEnvironments(org)
	if !ok {
		return nil, ErrOrganizationNotFound
	}

	names := sortedKeys(environments)
	out := make([]Document, 0, len(names))
	builder := NewDocumentBuilder()
	for _, name := range names {
		env, _, exists := i.state.GetEnvironment(org, name)
		if !exists {
			continue
		}
		out = append(out, builder.Environment(org, env))
	}
	return out, nil
}

func (i MemoryIndex) nodeDocuments(org string) ([]Document, error) {
	nodes, ok := i.state.ListNodes(org)
	if !ok {
		return nil, ErrOrganizationNotFound
	}

	names := sortedKeys(nodes)
	out := make([]Document, 0, len(names))
	builder := NewDocumentBuilder()
	for _, name := range names {
		node, _, exists := i.state.GetNode(org, name)
		if !exists {
			continue
		}
		out = append(out, builder.Node(org, node))
	}
	return out, nil
}

func (i MemoryIndex) roleDocuments(org string) ([]Document, error) {
	roles, ok := i.state.ListRoles(org)
	if !ok {
		return nil, ErrOrganizationNotFound
	}

	names := sortedKeys(roles)
	out := make([]Document, 0, len(names))
	builder := NewDocumentBuilder()
	for _, name := range names {
		role, _, exists := i.state.GetRole(org, name)
		if !exists {
			continue
		}
		out = append(out, builder.Role(org, role))
	}
	return out, nil
}

func (i MemoryIndex) dataBagDocuments(org, bagName string) ([]Document, error) {
	items, orgExists, bagExists := i.state.ListDataBagItems(org, bagName)
	switch {
	case !orgExists:
		return nil, ErrOrganizationNotFound
	case !bagExists:
		return nil, ErrIndexNotFound
	}

	names := sortedKeys(items)
	out := make([]Document, 0, len(names))
	builder := NewDocumentBuilder()
	for _, name := range names {
		item, itemOrgExists, itemBagExists, itemExists := i.state.GetDataBagItem(org, bagName, name)
		switch {
		case !itemOrgExists:
			return nil, ErrOrganizationNotFound
		case !itemBagExists:
			return nil, ErrIndexNotFound
		case !itemExists:
			continue
		}

		out = append(out, builder.DataBagItem(org, bagName, item))
	}
	return out, nil
}

func sortedKeys(in map[string]string) []string {
	keys := make([]string, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
