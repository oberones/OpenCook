package search

import (
	"context"
	"strings"

	"github.com/oberones/OpenCook/internal/bootstrap"
)

const openSearchDefaultSearchRows = 10000

type OpenSearchIndex struct {
	state  *bootstrap.Service
	client *OpenSearchClient
	target string
}

func NewOpenSearchIndex(state *bootstrap.Service, client *OpenSearchClient, target string) *OpenSearchIndex {
	return &OpenSearchIndex{
		state:  state,
		client: client,
		target: strings.TrimSpace(target),
	}
}

func (i *OpenSearchIndex) Name() string {
	return "opensearch"
}

func (i *OpenSearchIndex) Status() Status {
	if i == nil || i.state == nil || i.client == nil {
		return OpenSearchUnavailableStatus()
	}
	return OpenSearchActiveStatus()
}

func (i *OpenSearchIndex) Indexes(_ context.Context, org string) ([]string, error) {
	if i == nil || i.state == nil || i.client == nil {
		return nil, ErrUnavailable
	}
	return searchIndexesForState(i.state, org)
}

func (i *OpenSearchIndex) Search(ctx context.Context, query Query) (Result, error) {
	if i == nil || i.state == nil || i.client == nil {
		return Result{}, ErrUnavailable
	}

	query.Organization = strings.TrimSpace(query.Organization)
	query.Index = strings.TrimSpace(query.Index)
	if err := ensureSearchOrganization(i.state, query.Organization); err != nil {
		return Result{}, err
	}
	if !i.indexExists(query.Organization, query.Index) {
		return Result{}, ErrIndexNotFound
	}

	ids, err := i.client.SearchIDs(ctx, query, 0, openSearchDefaultSearchRows)
	if err != nil {
		return Result{}, err
	}

	docs := make([]Document, 0, len(ids))
	for _, id := range ids {
		ref, ok := parseOpenSearchDocumentID(id)
		if !ok || ref.Organization != query.Organization || ref.Index != query.Index {
			continue
		}
		doc, found, err := i.documentForRef(ref)
		if err != nil {
			return Result{}, err
		}
		if !found {
			continue
		}
		docs = append(docs, doc)
	}
	return Result{Documents: docs}, nil
}

func (i *OpenSearchIndex) indexExists(org, index string) bool {
	if strings.TrimSpace(index) == "" {
		return false
	}
	for _, builtIn := range builtInIndexes {
		if index == builtIn {
			return true
		}
	}
	_, orgExists, bagExists := i.state.GetDataBag(org, index)
	return orgExists && bagExists
}

func (i *OpenSearchIndex) documentForRef(ref DocumentRef) (Document, bool, error) {
	builder := NewDocumentBuilder()
	switch ref.Index {
	case "client":
		client, found := i.state.GetClient(ref.Organization, ref.Name)
		if !found {
			return Document{}, false, nil
		}
		return builder.Client(ref.Organization, client), true, nil
	case "environment":
		env, orgExists, found := i.state.GetEnvironment(ref.Organization, ref.Name)
		if !orgExists {
			return Document{}, false, ErrOrganizationNotFound
		}
		if !found {
			return Document{}, false, nil
		}
		return builder.Environment(ref.Organization, env), true, nil
	case "node":
		node, orgExists, found := i.state.GetNode(ref.Organization, ref.Name)
		if !orgExists {
			return Document{}, false, ErrOrganizationNotFound
		}
		if !found {
			return Document{}, false, nil
		}
		return builder.Node(ref.Organization, node), true, nil
	case "role":
		role, orgExists, found := i.state.GetRole(ref.Organization, ref.Name)
		if !orgExists {
			return Document{}, false, ErrOrganizationNotFound
		}
		if !found {
			return Document{}, false, nil
		}
		return builder.Role(ref.Organization, role), true, nil
	default:
		item, orgExists, bagExists, found := i.state.GetDataBagItem(ref.Organization, ref.Index, ref.Name)
		if !orgExists {
			return Document{}, false, ErrOrganizationNotFound
		}
		if !bagExists || !found {
			return Document{}, false, nil
		}
		return builder.DataBagItem(ref.Organization, ref.Index, item), true, nil
	}
}

func searchIndexesForState(state *bootstrap.Service, org string) ([]string, error) {
	if state == nil {
		return nil, ErrUnavailable
	}
	if err := ensureSearchOrganization(state, org); err != nil {
		return nil, err
	}

	indexes := append([]string(nil), builtInIndexes...)
	seen := make(map[string]struct{}, len(indexes))
	for _, name := range indexes {
		seen[name] = struct{}{}
	}
	dataBags, ok := state.ListDataBags(org)
	if !ok {
		return nil, ErrOrganizationNotFound
	}
	for _, name := range sortedKeys(dataBags) {
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		indexes = append(indexes, name)
	}
	return indexes, nil
}

func ensureSearchOrganization(state *bootstrap.Service, org string) error {
	if state == nil {
		return ErrUnavailable
	}
	if strings.TrimSpace(org) == "" {
		return ErrOrganizationNotFound
	}
	if _, ok := state.GetOrganization(org); !ok {
		return ErrOrganizationNotFound
	}
	return nil
}

func parseOpenSearchDocumentID(id string) (DocumentRef, bool) {
	parts := strings.Split(strings.TrimSpace(id), "/")
	if len(parts) != 3 {
		return DocumentRef{}, false
	}
	ref := DocumentRef{
		Organization: strings.TrimSpace(parts[0]),
		Index:        strings.TrimSpace(parts[1]),
		Name:         strings.TrimSpace(parts[2]),
	}
	if ref.Organization == "" || ref.Index == "" || ref.Name == "" {
		return DocumentRef{}, false
	}
	return ref, true
}
