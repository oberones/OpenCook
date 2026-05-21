package api

import "net/http"

// writeCoreObjectMethodNotAllowed keeps the collection-vs-named object Allow
// header logic consistent for Chef object routes that share the same shape.
func writeCoreObjectMethodNotAllowed(w http.ResponseWriter, path, basePath, collectionMessage, namedMessage string) {
	if matchesCollectionPath(path, basePath) {
		writeMethodNotAllowed(w, collectionMessage, http.MethodGet, http.MethodHead, http.MethodPost)
		return
	}
	if _, ok := pathTail(path, basePath+"/"); ok {
		writeMethodNotAllowed(w, namedMessage, http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete)
		return
	}

	writeJSON(w, http.StatusNotFound, apiError{
		Error:   "not_found",
		Message: "route not found in scaffold router",
	})
}
