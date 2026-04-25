package search

import (
	"context"

	"github.com/oberones/OpenCook/internal/bootstrap"
)

func RebuildOpenSearchIndex(ctx context.Context, client *OpenSearchClient, state *bootstrap.Service) error {
	service := NewReindexService(state, client)
	_, err := service.Run(ctx, ReindexPlan{
		Mode:             ReindexModeComplete,
		AllOrganizations: true,
	})
	return err
}

func DocumentsFromBootstrapState(state *bootstrap.Service) ([]Document, error) {
	return DocumentsFromBootstrapStateForPlan(state, ReindexPlan{AllOrganizations: true})
}

func DocumentsFromBootstrapStateForPlan(state *bootstrap.Service, plan ReindexPlan) ([]Document, error) {
	docs, err := documentsFromBootstrapState(state)
	if err != nil {
		return nil, err
	}
	filtered, _, err := filterReindexDocuments(state, docs, normalizeReindexPlan(plan))
	return filtered, err
}

func documentsFromBootstrapState(state *bootstrap.Service) ([]Document, error) {
	if state == nil {
		return nil, ErrUnavailable
	}

	builder := NewDocumentBuilder()
	orgs := state.ListOrganizations()
	docs := make([]Document, 0)
	for _, orgName := range sortedKeys(orgs) {
		clients, ok := state.ListClients(orgName)
		if !ok {
			return nil, ErrOrganizationNotFound
		}
		for _, name := range sortedKeys(clients) {
			client, exists := state.GetClient(orgName, name)
			if !exists {
				continue
			}
			docs = append(docs, builder.Client(orgName, client))
		}

		environments, ok := state.ListEnvironments(orgName)
		if !ok {
			return nil, ErrOrganizationNotFound
		}
		for _, name := range sortedKeys(environments) {
			env, _, exists := state.GetEnvironment(orgName, name)
			if !exists {
				continue
			}
			docs = append(docs, builder.Environment(orgName, env))
		}

		nodes, ok := state.ListNodes(orgName)
		if !ok {
			return nil, ErrOrganizationNotFound
		}
		for _, name := range sortedKeys(nodes) {
			node, _, exists := state.GetNode(orgName, name)
			if !exists {
				continue
			}
			docs = append(docs, builder.Node(orgName, node))
		}

		roles, ok := state.ListRoles(orgName)
		if !ok {
			return nil, ErrOrganizationNotFound
		}
		for _, name := range sortedKeys(roles) {
			role, _, exists := state.GetRole(orgName, name)
			if !exists {
				continue
			}
			docs = append(docs, builder.Role(orgName, role))
		}

		dataBags, ok := state.ListDataBags(orgName)
		if !ok {
			return nil, ErrOrganizationNotFound
		}
		for _, bagName := range sortedKeys(dataBags) {
			items, orgExists, bagExists := state.ListDataBagItems(orgName, bagName)
			switch {
			case !orgExists:
				return nil, ErrOrganizationNotFound
			case !bagExists:
				continue
			}
			for _, itemID := range sortedKeys(items) {
				item, itemOrgExists, itemBagExists, itemExists := state.GetDataBagItem(orgName, bagName, itemID)
				switch {
				case !itemOrgExists:
					return nil, ErrOrganizationNotFound
				case !itemBagExists:
					continue
				case !itemExists:
					continue
				}
				docs = append(docs, builder.DataBagItem(orgName, bagName, item))
			}
		}
	}
	return docs, nil
}
