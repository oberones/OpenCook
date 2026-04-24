package bootstrap

import (
	"regexp"
	"sort"
	"strings"
	"time"
)

var validSandboxChecksumPattern = regexp.MustCompile(`^[A-Fa-f0-9]{32}$`)

type Sandbox struct {
	ID           string    `json:"sandbox_id"`
	Organization string    `json:"-"`
	Checksums    []string  `json:"checksums"`
	CreatedAt    time.Time `json:"-"`
}

type CreateSandboxInput struct {
	Checksums []string
}

func ValidSandboxChecksum(value string) bool {
	return validSandboxChecksumPattern.MatchString(strings.TrimSpace(value))
}

func (s *Service) CreateSandbox(orgName string, input CreateSandboxInput) (Sandbox, error) {
	checksums, err := normalizeSandboxChecksums(input.Checksums)
	if err != nil {
		return Sandbox{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return Sandbox{}, ErrNotFound
	}

	sandbox := Sandbox{
		ID:           newGUID(),
		Organization: orgName,
		Checksums:    checksums,
		CreatedAt:    time.Now().UTC(),
	}
	previous := s.snapshotCoreObjectsLocked()
	org.sandboxes[sandbox.ID] = sandbox
	if err := s.finishCoreObjectMutationLocked(previous); err != nil {
		return Sandbox{}, err
	}
	return copySandbox(sandbox), nil
}

func (s *Service) GetSandbox(orgName, sandboxID string) (Sandbox, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return Sandbox{}, false, false
	}

	sandbox, ok := org.sandboxes[strings.TrimSpace(sandboxID)]
	if !ok {
		return Sandbox{}, true, false
	}

	return copySandbox(sandbox), true, true
}

func (s *Service) DeleteSandbox(orgName, sandboxID string) (Sandbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return Sandbox{}, ErrNotFound
	}

	sandbox, ok := org.sandboxes[strings.TrimSpace(sandboxID)]
	if !ok {
		return Sandbox{}, ErrNotFound
	}

	previous := s.snapshotCoreObjectsLocked()
	delete(org.sandboxes, sandbox.ID)
	if err := s.finishCoreObjectMutationLocked(previous); err != nil {
		return Sandbox{}, err
	}
	return copySandbox(sandbox), nil
}

func normalizeSandboxChecksums(checksums []string) ([]string, error) {
	if len(checksums) == 0 {
		return nil, ErrInvalidInput
	}

	seen := make(map[string]struct{}, len(checksums))
	out := make([]string, 0, len(checksums))
	for _, checksum := range checksums {
		checksum = strings.ToLower(strings.TrimSpace(checksum))
		if !ValidSandboxChecksum(checksum) {
			return nil, ErrInvalidInput
		}
		if _, ok := seen[checksum]; ok {
			continue
		}
		seen[checksum] = struct{}{}
		out = append(out, checksum)
	}

	sort.Strings(out)
	if len(out) == 0 {
		return nil, ErrInvalidInput
	}
	return out, nil
}

func copySandbox(in Sandbox) Sandbox {
	return Sandbox{
		ID:           in.ID,
		Organization: in.Organization,
		Checksums:    append([]string(nil), in.Checksums...),
		CreatedAt:    in.CreatedAt,
	}
}
