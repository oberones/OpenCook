# Chef Source Import Fixture V1

This fixture freezes the first normalized Chef Infra Server source contract for
OpenCook migration planning and functional Docker coverage. It is intentionally
tiny and synthetic: each payload file represents one Chef-visible family without
depending on private upstream database schemas or live Chef Server services.

The fixture is read-only. Source inventory uses it to recognize the normalized
manifest, payload family taxonomy, checksum blob side channel, derived
OpenSearch side channel, and unsupported ancillary source families. The
functional migration phases also normalize, import, sync, shadow-compare, and
rehearse cutover from this fixture against Docker-managed PostgreSQL,
OpenSearch, and provider-backed blob volumes.
