package api

import (
	"errors"
	"net/http"
	"sort"
	"strings"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
)

func (s *server) handleData(w http.ResponseWriter, r *http.Request) {
	state := s.deps.Bootstrap
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	org, basePath, ok := s.resolveDataBagRoute(w, r)
	if !ok {
		return
	}
	if _, exists := state.GetOrganization(org); !exists {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	}

	parts, ok := dataBagPathSegments(r.URL.Path, basePath)
	if !ok {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}

	switch len(parts) {
	case 0:
		s.handleDataBagCollection(w, r, state, org, basePath)
	case 1:
		s.handleNamedDataBag(w, r, state, org, basePath, parts[0])
	case 2:
		s.handleDataBagItem(w, r, state, org, basePath, parts[0], parts[1])
	default:
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
	}
}

func (s *server) handleDataBagCollection(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	switch r.Method {
	case http.MethodGet:
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "container",
			Name:         "data",
			Organization: org,
		}) {
			return
		}

		bags, _ := state.ListDataBags(org)
		if bags == nil {
			bags = map[string]string{}
		}
		response := make(map[string]string, len(bags))
		for name := range bags {
			response[name] = dataBagURLForResponse(org, name, basePath)
		}
		writeJSON(w, http.StatusOK, response)
	case http.MethodPost:
		if !s.authorizeRequest(w, r, authz.ActionCreate, authz.Resource{
			Type:         "container",
			Name:         "data",
			Organization: org,
		}) {
			return
		}

		var payload map[string]any
		if !decodeJSON(w, r, &payload) {
			return
		}

		requestor, _ := requestorFromContext(r.Context())
		bag, err := state.CreateDataBag(org, bootstrap.CreateDataBagInput{
			Payload: payload,
			Creator: requestor,
		})
		if !s.writeDataBagError(w, err, bag.Name) {
			return
		}

		writeJSON(w, http.StatusCreated, map[string]any{
			"uri": dataBagURLForResponse(org, bag.Name, basePath),
		})
	default:
		writeMethodNotAllowed(w, "method not allowed for data route", http.MethodGet, http.MethodPost)
	}
}

func (s *server) handleNamedDataBag(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath, bagName string) {
	switch r.Method {
	case http.MethodGet:
		_, orgExists, bagExists := state.GetDataBag(org, bagName)
		if !orgExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "organization not found",
			})
			return
		}
		if !bagExists {
			writeDataBagMessages(w, http.StatusNotFound, cannotLoadDataBagMessage(bagName))
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "data_bag",
			Name:         bagName,
			Organization: org,
		}) {
			return
		}

		items, _, _ := state.ListDataBagItems(org, bagName)
		if items == nil {
			items = map[string]string{}
		}
		response := make(map[string]string, len(items))
		for id := range items {
			response[id] = dataBagItemURLForResponse(org, bagName, id, basePath)
		}
		writeJSON(w, http.StatusOK, response)
	case http.MethodPost:
		_, orgExists, bagExists := state.GetDataBag(org, bagName)
		if !orgExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "organization not found",
			})
			return
		}
		if !bagExists {
			writeDataBagMessages(w, http.StatusNotFound, missingDataBagForItemCreateMessage(bagName))
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionCreate, authz.Resource{
			Type:         "data_bag",
			Name:         bagName,
			Organization: org,
		}) {
			return
		}

		var payload map[string]any
		if !decodeJSON(w, r, &payload) {
			return
		}

		itemID := dataBagItemIDHint(payload)
		item, err := state.CreateDataBagItem(org, bagName, bootstrap.CreateDataBagItemInput{Payload: payload})
		if !s.writeDataBagItemCreateError(w, err, bagName, itemID) {
			return
		}

		writeJSON(w, http.StatusCreated, dataBagItemResponse(bagName, item.RawData))
	case http.MethodDelete:
		_, orgExists, bagExists := state.GetDataBag(org, bagName)
		if !orgExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "organization not found",
			})
			return
		}
		if !bagExists {
			writeDataBagMessages(w, http.StatusNotFound, cannotLoadDataBagMessage(bagName))
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionDelete, authz.Resource{
			Type:         "data_bag",
			Name:         bagName,
			Organization: org,
		}) {
			return
		}

		bag, err := state.DeleteDataBag(org, bagName)
		if !s.writeDataBagError(w, err, bagName) {
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"name":       bag.Name,
			"json_class": bag.JSONClass,
			"chef_type":  bag.ChefType,
		})
	default:
		writeMethodNotAllowed(w, "method not allowed for data bag route", http.MethodGet, http.MethodPost, http.MethodDelete)
	}
}

func (s *server) handleDataBagItem(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath, bagName, itemID string) {
	switch r.Method {
	case http.MethodGet:
		item, orgExists, bagExists, itemExists := state.GetDataBagItem(org, bagName, itemID)
		if !orgExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "organization not found",
			})
			return
		}
		if !bagExists || !itemExists {
			writeDataBagMessages(w, http.StatusNotFound, cannotLoadDataBagItemForDataBagMessage(bagName, itemID))
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "data_bag",
			Name:         bagName,
			Organization: org,
		}) {
			return
		}

		writeJSON(w, http.StatusOK, item.RawData)
	case http.MethodPut:
		_, orgExists, bagExists, itemExists := state.GetDataBagItem(org, bagName, itemID)
		if !orgExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "organization not found",
			})
			return
		}
		if !bagExists || !itemExists {
			writeDataBagMessages(w, http.StatusNotFound, cannotLoadDataBagItemForDataBagMessage(bagName, itemID))
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionUpdate, authz.Resource{
			Type:         "data_bag",
			Name:         bagName,
			Organization: org,
		}) {
			return
		}

		var payload map[string]any
		if !decodeJSON(w, r, &payload) {
			return
		}

		item, err := state.UpdateDataBagItem(org, bagName, itemID, bootstrap.UpdateDataBagItemInput{Payload: payload})
		if !s.writeDataBagItemUpdateError(w, err, bagName, itemID) {
			return
		}

		writeJSON(w, http.StatusOK, dataBagItemResponse(bagName, item.RawData))
	case http.MethodDelete:
		_, orgExists, bagExists, itemExists := state.GetDataBagItem(org, bagName, itemID)
		if !orgExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "organization not found",
			})
			return
		}
		if !bagExists || !itemExists {
			writeDataBagMessages(w, http.StatusNotFound, cannotLoadDataBagItemMessage(bagName, itemID))
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionDelete, authz.Resource{
			Type:         "data_bag",
			Name:         bagName,
			Organization: org,
		}) {
			return
		}

		item, err := state.DeleteDataBagItem(org, bagName, itemID)
		if !s.writeDataBagItemDeleteError(w, err, bagName, itemID) {
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"name":       "data_bag_item_" + bagName + "_" + itemID,
			"json_class": "Chef::DataBagItem",
			"chef_type":  "data_bag_item",
			"data_bag":   bagName,
			"raw_data":   item.RawData,
		})
	default:
		writeMethodNotAllowed(w, "method not allowed for data bag item route", http.MethodGet, http.MethodPut, http.MethodDelete)
	}
}

func (s *server) resolveDataBagRoute(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	org := strings.TrimSpace(r.PathValue("org"))
	if org != "" {
		return org, "/organizations/" + org + "/data", true
	}

	org, ok := s.resolveDefaultOrganizationName()
	if !ok {
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "organization_required",
			Message: "organization context is required for this route",
		})
		return "", "", false
	}

	return org, "/data", true
}

func dataBagPathSegments(path, basePath string) ([]string, bool) {
	if matchesCollectionPath(path, basePath) {
		return nil, true
	}

	tail := strings.TrimPrefix(path, basePath+"/")
	tail = strings.TrimSuffix(tail, "/")
	if tail == "" || strings.Contains(tail, "//") {
		return nil, false
	}

	parts := strings.Split(tail, "/")
	if len(parts) == 0 || len(parts) > 2 {
		return nil, false
	}
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			return nil, false
		}
	}
	return parts, true
}

func dataBagURLForResponse(org, bagName, basePath string) string {
	if basePath == "/data" {
		return "/data/" + bagName
	}
	return "/organizations/" + org + "/data/" + bagName
}

func dataBagItemURLForResponse(org, bagName, itemID, basePath string) string {
	return dataBagURLForResponse(org, bagName, basePath) + "/" + itemID
}

func dataBagItemResponse(bagName string, raw map[string]any) map[string]any {
	response := cloneResponseMap(raw)
	response["chef_type"] = "data_bag_item"
	response["data_bag"] = bagName
	return response
}

func cloneResponseMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}

	out := make(map[string]any, len(in))
	keys := make([]string, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		out[key] = cloneResponseValue(in[key])
	}
	return out
}

func cloneResponseValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneResponseMap(typed)
	case []any:
		out := make([]any, len(typed))
		for idx := range typed {
			out[idx] = cloneResponseValue(typed[idx])
		}
		return out
	case []string:
		return append([]string(nil), typed...)
	default:
		return typed
	}
}

func dataBagItemIDHint(payload map[string]any) string {
	if payload == nil {
		return ""
	}

	rawID, ok := payload["id"]
	if !ok {
		return ""
	}

	id, ok := rawID.(string)
	if !ok {
		return ""
	}

	return strings.TrimSpace(id)
}

func (s *server) writeDataBagError(w http.ResponseWriter, err error, bagName string) bool {
	if err == nil {
		return true
	}

	var validationErr *bootstrap.ValidationError
	switch {
	case errors.As(err, &validationErr):
		writeDataBagMessages(w, http.StatusBadRequest, validationErr.Messages...)
	case errors.Is(err, bootstrap.ErrConflict):
		writeDataBagMessages(w, http.StatusConflict, "Data bag already exists")
	case errors.Is(err, bootstrap.ErrNotFound):
		writeDataBagMessages(w, http.StatusNotFound, cannotLoadDataBagMessage(bagName))
	default:
		s.logf("data bag compatibility failure: %v", err)
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "data_bag_failed",
			Message: "internal data bag compatibility error",
		})
	}
	return false
}

func (s *server) writeDataBagItemCreateError(w http.ResponseWriter, err error, bagName, itemID string) bool {
	if err == nil {
		return true
	}

	var validationErr *bootstrap.ValidationError
	switch {
	case errors.As(err, &validationErr):
		writeDataBagMessages(w, http.StatusBadRequest, validationErr.Messages...)
	case errors.Is(err, bootstrap.ErrConflict):
		writeDataBagMessages(w, http.StatusConflict, dataBagItemAlreadyExistsMessage(bagName, itemID))
	case errors.Is(err, bootstrap.ErrNotFound):
		writeDataBagMessages(w, http.StatusNotFound, missingDataBagForItemCreateMessage(bagName))
	default:
		s.logf("data bag item create failure: %v", err)
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "data_bag_item_failed",
			Message: "internal data bag item compatibility error",
		})
	}
	return false
}

func (s *server) writeDataBagItemUpdateError(w http.ResponseWriter, err error, bagName, itemID string) bool {
	if err == nil {
		return true
	}

	var validationErr *bootstrap.ValidationError
	switch {
	case errors.As(err, &validationErr):
		writeDataBagMessages(w, http.StatusBadRequest, validationErr.Messages...)
	case errors.Is(err, bootstrap.ErrNotFound):
		writeDataBagMessages(w, http.StatusNotFound, cannotLoadDataBagItemForDataBagMessage(bagName, itemID))
	default:
		s.logf("data bag item update failure: %v", err)
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "data_bag_item_failed",
			Message: "internal data bag item compatibility error",
		})
	}
	return false
}

func (s *server) writeDataBagItemDeleteError(w http.ResponseWriter, err error, bagName, itemID string) bool {
	if err == nil {
		return true
	}

	switch {
	case errors.Is(err, bootstrap.ErrNotFound):
		writeDataBagMessages(w, http.StatusNotFound, cannotLoadDataBagItemMessage(bagName, itemID))
	default:
		s.logf("data bag item delete failure: %v", err)
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "data_bag_item_failed",
			Message: "internal data bag item compatibility error",
		})
	}
	return false
}

func writeDataBagMessages(w http.ResponseWriter, status int, messages ...string) {
	writeJSON(w, status, map[string]any{
		"error": messages,
	})
}

func writeMethodNotAllowed(w http.ResponseWriter, message string, allowed ...string) {
	w.Header().Set("Allow", strings.Join(allowed, ", "))
	writeJSON(w, http.StatusMethodNotAllowed, apiError{
		Error:   "method_not_allowed",
		Message: message,
	})
}

func cannotLoadDataBagMessage(bagName string) string {
	return "Cannot load data bag " + bagName
}

func cannotLoadDataBagItemMessage(bagName, itemID string) string {
	return "Cannot load data bag " + bagName + " item " + itemID
}

func cannotLoadDataBagItemForDataBagMessage(bagName, itemID string) string {
	return "Cannot load data bag item " + itemID + " for data bag " + bagName
}

func missingDataBagForItemCreateMessage(bagName string) string {
	return "No data bag '" + bagName + "' could be found. Please create this data bag before adding items to it."
}

func dataBagItemAlreadyExistsMessage(bagName, itemID string) string {
	return "Data Bag Item '" + itemID + "' already exists in Data Bag '" + bagName + "'."
}
