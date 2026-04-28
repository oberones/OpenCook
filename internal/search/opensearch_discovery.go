package search

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

// OpenSearchProviderInfo records the provider identity and inferred feature
// set discovered from the configured OpenSearch-compatible endpoint.
type OpenSearchProviderInfo struct {
	Distribution  string
	Version       string
	VersionParsed bool
	Major         int
	Minor         int
	Patch         int
	Tagline       string
	NodeName      string
	ClusterName   string
	BuildFlavor   string
	BuildType     string
	BuildHash     string
	Capabilities  OpenSearchCapabilities
}

// OpenSearchCapabilities describes provider operations OpenCook relies on.
// Later hardening tasks can replace inferred flags with more exact probes.
type OpenSearchCapabilities struct {
	IndexExistsChecks             bool
	CreateIndex                   bool
	PutMapping                    bool
	BulkIndexing                  bool
	SearchIDs                     bool
	SearchAfterPagination         bool
	Refresh                       bool
	DeleteDocument                bool
	DeleteByQuery                 bool
	DeleteByQueryFallbackRequired bool
	TotalHitsObjectResponses      bool
}

type openSearchRootResponse struct {
	Name        string `json:"name"`
	ClusterName string `json:"cluster_name"`
	Tagline     string `json:"tagline"`
	Version     struct {
		Distribution string `json:"distribution"`
		Number       string `json:"number"`
		BuildFlavor  string `json:"build_flavor"`
		BuildType    string `json:"build_type"`
		BuildHash    string `json:"build_hash"`
	} `json:"version"`
}

// DiscoverProvider performs the non-mutating discovery sequence used to model
// the configured provider before activation, status, or admin workflows rely on
// version-specific behavior.
func (c *OpenSearchClient) DiscoverProvider(ctx context.Context) (OpenSearchProviderInfo, error) {
	if c == nil {
		return OpenSearchProviderInfo{}, fmt.Errorf("%w: opensearch client is nil", ErrInvalidConfiguration)
	}

	info, err := c.discoverProviderIdentity(ctx)
	if err != nil {
		return OpenSearchProviderInfo{}, err
	}
	if err := c.discoverIndexExistsCapability(ctx); err != nil {
		return OpenSearchProviderInfo{}, err
	}
	info.Capabilities = inferOpenSearchCapabilities(info)
	if err := validateOpenSearchCapabilities(info); err != nil {
		return OpenSearchProviderInfo{}, err
	}
	c.setProviderInfo(info)
	return info, nil
}

func (c *OpenSearchClient) discoverProviderIdentity(ctx context.Context) (OpenSearchProviderInfo, error) {
	req, err := c.newRequest(ctx, http.MethodGet, "/", nil, nil, "")
	if err != nil {
		return OpenSearchProviderInfo{}, err
	}
	resp, err := c.do(req)
	if err != nil {
		return OpenSearchProviderInfo{}, err
	}
	defer closeResponseBody(resp)
	if err := classifyOpenSearchResponse(resp, http.StatusOK); err != nil {
		return OpenSearchProviderInfo{}, err
	}

	var payload openSearchRootResponse
	if err := decodeOpenSearchJSON(resp.Body, &payload, "provider discovery response"); err != nil {
		return OpenSearchProviderInfo{}, err
	}

	major, minor, patch, parsed := parseProviderVersion(payload.Version.Number)
	return OpenSearchProviderInfo{
		Distribution:  inferOpenSearchDistribution(payload.Version.Distribution, payload.Tagline),
		Version:       strings.TrimSpace(payload.Version.Number),
		VersionParsed: parsed,
		Major:         major,
		Minor:         minor,
		Patch:         patch,
		Tagline:       strings.TrimSpace(payload.Tagline),
		NodeName:      strings.TrimSpace(payload.Name),
		ClusterName:   strings.TrimSpace(payload.ClusterName),
		BuildFlavor:   strings.TrimSpace(payload.Version.BuildFlavor),
		BuildType:     strings.TrimSpace(payload.Version.BuildType),
		BuildHash:     strings.TrimSpace(payload.Version.BuildHash),
	}, nil
}

func (c *OpenSearchClient) discoverIndexExistsCapability(ctx context.Context) error {
	req, err := c.newRequest(ctx, http.MethodHead, "/"+openSearchChefIndex, nil, nil, "")
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer closeResponseBody(resp)
	return classifyOpenSearchResponse(resp, http.StatusOK, http.StatusNotFound)
}

func (c *OpenSearchClient) setProviderInfo(info OpenSearchProviderInfo) {
	c.providerInfoMu.Lock()
	defer c.providerInfoMu.Unlock()
	c.providerInfo = info
	c.providerInfoKnown = true
}

func inferOpenSearchDistribution(distribution, tagline string) string {
	distribution = strings.ToLower(strings.TrimSpace(distribution))
	if distribution != "" {
		return distribution
	}
	lowerTagline := strings.ToLower(strings.TrimSpace(tagline))
	switch {
	case strings.Contains(lowerTagline, "opensearch"):
		return "opensearch"
	case strings.Contains(lowerTagline, "you know, for search"):
		return "elasticsearch"
	default:
		return "unknown"
	}
}

func inferOpenSearchCapabilities(info OpenSearchProviderInfo) OpenSearchCapabilities {
	caps := OpenSearchCapabilities{
		IndexExistsChecks:        true,
		CreateIndex:              true,
		PutMapping:               true,
		BulkIndexing:             true,
		SearchIDs:                true,
		SearchAfterPagination:    true,
		Refresh:                  true,
		DeleteDocument:           true,
		DeleteByQuery:            true,
		TotalHitsObjectResponses: true,
	}
	if info.Distribution == "elasticsearch" && info.VersionParsed {
		if info.Major < 7 {
			caps.TotalHitsObjectResponses = false
		}
		if info.Major < 5 {
			caps.SearchAfterPagination = false
			caps.DeleteByQuery = false
		}
	}
	return caps
}

// validateOpenSearchCapabilities rejects providers that cannot satisfy the
// shared search contract before OpenCook advertises active OpenSearch mode.
func validateOpenSearchCapabilities(info OpenSearchProviderInfo) error {
	caps := info.Capabilities
	identity := openSearchProviderIdentity(info)
	if !caps.SearchAfterPagination {
		return fmt.Errorf("%w: provider %s does not support required search_after pagination", ErrInvalidConfiguration, identity)
	}
	if !caps.DeleteByQuery && !caps.DeleteByQueryFallbackRequired {
		return fmt.Errorf("%w: provider %s does not support delete-by-query or a safe fallback", ErrInvalidConfiguration, identity)
	}
	if caps.DeleteByQueryFallbackRequired && !caps.SearchAfterPagination {
		return fmt.Errorf("%w: provider %s delete-by-query fallback requires search_after pagination", ErrInvalidConfiguration, identity)
	}
	return nil
}

func openSearchProviderIdentity(info OpenSearchProviderInfo) string {
	provider := strings.TrimSpace(info.Distribution)
	if provider == "" {
		provider = "unknown"
	}
	version := strings.TrimSpace(info.Version)
	if version == "" {
		return provider
	}
	return provider + " " + version
}

func openSearchProviderStatusMessage(info OpenSearchProviderInfo, known bool) string {
	if !known {
		return "OpenSearch-backed search provider active; provider discovery details unavailable"
	}

	provider := strings.TrimSpace(info.Distribution)
	if provider == "" {
		provider = "unknown"
	}
	version := strings.TrimSpace(info.Version)
	identity := provider
	if version != "" {
		identity += " " + version
	}

	prefix := "OpenSearch-backed search provider active"
	if provider != "opensearch" {
		prefix = "OpenSearch-compatible search provider active"
	}

	capabilitySummary := openSearchCapabilityStatusSummary(info.Capabilities)
	if capabilitySummary == "" {
		return prefix + " (" + identity + ")"
	}
	return prefix + " (" + identity + "; " + capabilitySummary + ")"
}

func openSearchCapabilityStatusSummary(caps OpenSearchCapabilities) string {
	parts := make([]string, 0, 3)
	if caps.SearchAfterPagination {
		parts = append(parts, "search-after pagination")
	}
	if caps.DeleteByQueryFallbackRequired {
		parts = append(parts, "delete-by-query fallback required")
	} else if caps.DeleteByQuery {
		parts = append(parts, "delete-by-query")
	}
	if caps.TotalHitsObjectResponses {
		parts = append(parts, "object total hits")
	} else {
		parts = append(parts, "legacy total hits")
	}
	return strings.Join(parts, ", ")
}

func parseProviderVersion(version string) (int, int, int, bool) {
	parts := strings.Split(strings.TrimSpace(version), ".")
	if len(parts) == 0 {
		return 0, 0, 0, false
	}
	major, ok := parseLeadingVersionNumber(parts[0])
	if !ok {
		return 0, 0, 0, false
	}
	minor := 0
	patch := 0
	if len(parts) > 1 {
		var parsed bool
		minor, parsed = parseLeadingVersionNumber(parts[1])
		if !parsed {
			return major, 0, 0, true
		}
	}
	if len(parts) > 2 {
		var parsed bool
		patch, parsed = parseLeadingVersionNumber(parts[2])
		if !parsed {
			return major, minor, 0, true
		}
	}
	return major, minor, patch, true
}

func parseLeadingVersionNumber(value string) (int, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	out := 0
	seen := false
	for _, r := range value {
		if r < '0' || r > '9' {
			break
		}
		seen = true
		out = out*10 + int(r-'0')
	}
	return out, seen
}
