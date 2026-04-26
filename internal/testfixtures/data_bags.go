// Package testfixtures contains shared compatibility payloads used by tests in
// multiple packages. Keeping these fixtures centralized helps each layer prove
// the same Chef-facing contract instead of inventing similar-but-different data.
package testfixtures

// EncryptedDataBagName returns the shared data bag name used by encrypted data
// bag compatibility tests across API, persistence, search, and functional layers.
func EncryptedDataBagName() string {
	return "encrypted_secrets"
}

// EncryptedDataBagItemID returns the canonical encrypted-looking item ID used
// by the shared encrypted data bag fixture.
func EncryptedDataBagItemID() string {
	return "database"
}

// OrdinaryDataBagItem returns a plain data bag item that documents the baseline
// OpenCook data bag contract before encrypted-looking envelopes are layered on.
func OrdinaryDataBagItem() map[string]any {
	return map[string]any{
		"id":   "plain",
		"kind": "ordinary",
		"nested": map[string]any{
			"owner":   "platform",
			"enabled": true,
		},
	}
}

// EncryptedDataBagItem returns a canonical encrypted-looking data bag item.
// The values are deterministic fake ciphertexts; tests use them to prove server
// opacity and JSON preservation, not cryptographic correctness.
func EncryptedDataBagItem() map[string]any {
	return map[string]any{
		"id":          EncryptedDataBagItemID(),
		"kind":        "database",
		"environment": "production",
		"password": map[string]any{
			"encrypted_data": "ZXhhbXBsZS1wYXNzd29yZC1jaXBoZXJ0ZXh0",
			"iv":             "cGFzc3dvcmQtaXY=",
			"version":        float64(1),
			"cipher":         "aes-256-cbc",
		},
		"api_key": map[string]any{
			"encrypted_data": "ZXhhbXBsZS1hcGkta2V5LWNpcGhlcnRleHQ=",
			"iv":             "YXBpLWtleS1pdi0xMg==",
			"auth_tag":       "YXBpLWtleS1hdXRoLXRhZw==",
			"version":        float64(3),
			"cipher":         "aes-256-gcm",
		},
		"rotation": map[string]any{
			"enabled":       true,
			"interval_days": float64(30),
			"notes":         nil,
		},
		"replicas": []any{"primary", "standby"},
	}
}

// UpdatedEncryptedDataBagItem returns the canonical update payload for the
// encrypted data bag fixture. The body intentionally omits an id so tests can
// pin Chef-compatible URL-derived item ID behavior on PUT.
func UpdatedEncryptedDataBagItem() map[string]any {
	return map[string]any{
		"kind":        "database",
		"environment": "staging",
		"password": map[string]any{
			"encrypted_data": "dXBkYXRlZC1wYXNzd29yZC1jaXBoZXJ0ZXh0",
			"iv":             "dXBkYXRlZC1wYXNzd29yZC1pdg==",
			"version":        float64(1),
			"cipher":         "aes-256-cbc",
		},
		"api_key": map[string]any{
			"encrypted_data": "dXBkYXRlZC1hcGkta2V5LWNpcGhlcnRleHQ=",
			"iv":             "dXBkYXRlZC1hcGktaXY=",
			"auth_tag":       "dXBkYXRlZC1hdXRoLXRhZw==",
			"version":        float64(3),
			"cipher":         "aes-256-gcm",
		},
	}
}

// NestedEncryptedDataBagItem returns an encrypted-looking item with arrays,
// nested envelopes, booleans, numbers, and nulls so persistence and response
// tests can catch accidental flattening or type loss.
func NestedEncryptedDataBagItem() map[string]any {
	return map[string]any{
		"id":   "nested",
		"kind": "nested-encrypted",
		"credentials": []any{
			map[string]any{
				"name": "primary",
				"value": map[string]any{
					"encrypted_data": "cHJpbWFyeS1jaXBoZXJ0ZXh0",
					"iv":             "cHJpbWFyeS1pdg==",
					"version":        float64(1),
					"cipher":         "aes-256-cbc",
				},
			},
			map[string]any{
				"name": "secondary",
				"value": map[string]any{
					"encrypted_data": "c2Vjb25kYXJ5LWNpcGhlcnRleHQ=",
					"iv":             "c2Vjb25kYXJ5LWl2",
					"auth_tag":       "c2Vjb25kYXJ5LXRhZw==",
					"version":        float64(3),
					"cipher":         "aes-256-gcm",
				},
			},
		},
		"metadata": map[string]any{
			"enabled": true,
			"weight":  float64(2),
			"note":    nil,
		},
	}
}

// CloneDataBagPayload returns a deep copy of a fixture payload so tests can
// mutate payloads without coupling later assertions to shared map state.
func CloneDataBagPayload(payload map[string]any) map[string]any {
	if payload == nil {
		return nil
	}
	out := make(map[string]any, len(payload))
	for key, value := range payload {
		out[key] = cloneDataBagFixtureValue(value)
	}
	return out
}

// cloneDataBagFixtureValue recursively copies JSON-like fixture values while
// leaving scalar values untouched.
func cloneDataBagFixtureValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return CloneDataBagPayload(typed)
	case []any:
		out := make([]any, len(typed))
		for idx := range typed {
			out[idx] = cloneDataBagFixtureValue(typed[idx])
		}
		return out
	default:
		return typed
	}
}
