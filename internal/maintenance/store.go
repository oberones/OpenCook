package maintenance

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	defaultMode           = "manual"
	reasonStatusRuneLimit = 160
	actorStatusRuneLimit  = 80
	redactionEllipsis     = "..."
)

var (
	ErrInvalidInput = errors.New("invalid maintenance input")
	validMode       = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]*$`)
)

type State struct {
	Enabled   bool       `json:"enabled"`
	Mode      string     `json:"mode,omitempty"`
	Reason    string     `json:"reason,omitempty"`
	Actor     string     `json:"actor,omitempty"`
	CreatedAt time.Time  `json:"created_at,omitempty"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

type SafeState struct {
	Enabled         bool       `json:"enabled"`
	Mode            string     `json:"mode,omitempty"`
	Reason          string     `json:"reason,omitempty"`
	ReasonTruncated bool       `json:"reason_truncated,omitempty"`
	Actor           string     `json:"actor,omitempty"`
	ActorTruncated  bool       `json:"actor_truncated,omitempty"`
	CreatedAt       time.Time  `json:"created_at,omitempty"`
	ExpiresAt       *time.Time `json:"expires_at,omitempty"`
}

type EnableInput struct {
	Mode      string
	Reason    string
	Actor     string
	CreatedAt time.Time
	ExpiresAt *time.Time
}

type DisableInput struct {
	Actor string
}

type CheckResult struct {
	State     State
	Active    bool
	Expired   bool
	CheckedAt time.Time
}

type Store interface {
	Read(context.Context) (State, error)
	Enable(context.Context, EnableInput) (State, error)
	Disable(context.Context, DisableInput) (State, error)
	Check(context.Context) (CheckResult, error)
}

type MemoryStore struct {
	mu    sync.RWMutex
	now   func() time.Time
	state State
}

type MemoryStoreOption func(*MemoryStore)

// NewMemoryStore returns a process-local maintenance store for standalone
// deployments and tests. PostgreSQL-backed deployments will replace this store
// in a later task so every OpenCook process observes the same write gate.
func NewMemoryStore(opts ...MemoryStoreOption) *MemoryStore {
	store := &MemoryStore{now: time.Now}
	for _, opt := range opts {
		if opt != nil {
			opt(store)
		}
	}
	if store.now == nil {
		store.now = time.Now
	}
	return store
}

// WithClock injects a deterministic clock for expiration tests and future
// admin-command tests that need stable maintenance timestamps.
func WithClock(now func() time.Time) MemoryStoreOption {
	return func(store *MemoryStore) {
		store.now = now
	}
}

// Read returns the current raw maintenance state without applying expiration.
// Keeping expired state visible lets status and admin output explain why writes
// are no longer blocked instead of silently losing operator context.
func (s *MemoryStore) Read(ctx context.Context) (State, error) {
	if err := ctx.Err(); err != nil {
		return State{}, err
	}
	if s == nil {
		return State{}, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return cloneState(s.state), nil
}

// Enable validates and stores a normalized active maintenance state. The update
// is all-or-nothing so invalid enable attempts cannot erase an existing window.
func (s *MemoryStore) Enable(ctx context.Context, input EnableInput) (State, error) {
	if err := ctx.Err(); err != nil {
		return State{}, err
	}
	if s == nil {
		return State{}, fmt.Errorf("maintenance store is required")
	}

	state, err := NormalizeEnableInput(input, s.now())
	if err != nil {
		return State{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = state
	return cloneState(s.state), nil
}

// Disable clears the active maintenance state. The operation is intentionally
// idempotent because operators should be able to safely retry cleanup steps.
func (s *MemoryStore) Disable(ctx context.Context, input DisableInput) (State, error) {
	if err := ctx.Err(); err != nil {
		return State{}, err
	}
	if s == nil {
		return State{}, nil
	}
	if _, err := NormalizeDisableInput(input); err != nil {
		return State{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = State{}
	return State{}, nil
}

// Check evaluates whether the current maintenance state should block writes at
// the current clock instant. Expired windows are reported as inactive while the
// stored state remains readable for truthful operator diagnostics.
func (s *MemoryStore) Check(ctx context.Context) (CheckResult, error) {
	if err := ctx.Err(); err != nil {
		return CheckResult{}, err
	}
	if s == nil {
		now := normalizeTime(time.Now())
		return CheckResult{CheckedAt: now}, nil
	}

	now := normalizeTime(s.now())
	s.mu.RLock()
	state := cloneState(s.state)
	s.mu.RUnlock()

	expired := state.ExpiredAt(now)
	return CheckResult{
		State:     state,
		Active:    state.Enabled && !expired,
		Expired:   expired,
		CheckedAt: now,
	}, nil
}

// NormalizeEnableInput canonicalizes operator-provided maintenance state before
// it is stored. The store keeps the full reason for auditability, while
// SafeStatus returns a bounded display copy for status surfaces.
func NormalizeEnableInput(input EnableInput, fallbackNow time.Time) (State, error) {
	mode, err := normalizeMode(input.Mode)
	if err != nil {
		return State{}, err
	}
	reason, err := normalizeReason(input.Reason)
	if err != nil {
		return State{}, err
	}
	actor, err := normalizeOperatorText(input.Actor)
	if err != nil {
		return State{}, err
	}

	createdAt := normalizeTime(input.CreatedAt)
	if createdAt.IsZero() {
		createdAt = normalizeTime(fallbackNow)
	}
	if createdAt.IsZero() {
		createdAt = normalizeTime(time.Now())
	}
	expiresAt := cloneTime(input.ExpiresAt)
	if expiresAt != nil {
		normalized := normalizeTime(*expiresAt)
		expiresAt = &normalized
		if !expiresAt.After(createdAt) {
			return State{}, fmt.Errorf("%w: expires_at must be after created_at", ErrInvalidInput)
		}
	}

	return State{
		Enabled:   true,
		Mode:      mode,
		Reason:    reason,
		Actor:     actor,
		CreatedAt: createdAt,
		ExpiresAt: expiresAt,
	}, nil
}

// NormalizeDisableInput validates optional disable metadata even though the
// current state model clears the active window. This gives CLI and PostgreSQL
// stores one shared validation rule for future audit/history fields.
func NormalizeDisableInput(input DisableInput) (DisableInput, error) {
	actor, err := normalizeOperatorText(input.Actor)
	if err != nil {
		return DisableInput{}, err
	}
	return DisableInput{Actor: actor}, nil
}

// ActiveAt reports whether the state should block mutating Chef-facing writes
// at the supplied time. It treats expiration as authoritative but does not
// mutate or clear the stored state.
func (s State) ActiveAt(now time.Time) bool {
	return s.Enabled && !s.ExpiredAt(now)
}

// ExpiredAt reports whether an enabled maintenance window has reached its
// expiration time. Disabled states are never considered expired.
func (s State) ExpiredAt(now time.Time) bool {
	if !s.Enabled || s.ExpiresAt == nil {
		return false
	}
	now = normalizeTime(now)
	return !s.ExpiresAt.After(now)
}

// SafeStatus returns a display-safe copy of maintenance state for status and
// admin output. It removes control-style whitespace and bounds operator text so
// future Chef-facing responses do not leak long notes or provider details.
func (s State) SafeStatus() SafeState {
	reason, reasonTruncated := safeStatusText(s.Reason, reasonStatusRuneLimit)
	actor, actorTruncated := safeStatusText(s.Actor, actorStatusRuneLimit)
	return SafeState{
		Enabled:         s.Enabled,
		Mode:            s.Mode,
		Reason:          reason,
		ReasonTruncated: reasonTruncated,
		Actor:           actor,
		ActorTruncated:  actorTruncated,
		CreatedAt:       normalizeTime(s.CreatedAt),
		ExpiresAt:       cloneTime(s.ExpiresAt),
	}
}

// CloneState returns a deep copy of maintenance state. PostgreSQL repositories
// use it to protect cached inactive-state values the same way MemoryStore
// protects values returned to callers.
func CloneState(state State) State {
	return cloneState(state)
}

// normalizeMode keeps maintenance modes stable for storage and CLI output while
// still allowing future task-specific modes without changing the schema.
func normalizeMode(mode string) (string, error) {
	mode = strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(mode))), "_")
	if mode == "" {
		mode = defaultMode
	}
	if !validMode.MatchString(mode) {
		return "", fmt.Errorf("%w: mode must contain only lowercase letters, numbers, dots, underscores, or hyphens", ErrInvalidInput)
	}
	return mode, nil
}

// normalizeReason trims invalid UTF-8 and surrounding whitespace while keeping
// the operator's full reason intact for audit-oriented stores.
func normalizeReason(reason string) (string, error) {
	reason = strings.TrimSpace(strings.ToValidUTF8(reason, ""))
	if reason == "" {
		return "", fmt.Errorf("%w: reason is required", ErrInvalidInput)
	}
	if strings.ContainsRune(reason, '\x00') {
		return "", fmt.Errorf("%w: reason contains invalid control data", ErrInvalidInput)
	}
	return reason, nil
}

// normalizeOperatorText validates optional actor text without forcing a future
// CLI to have an authenticated operator identity available.
func normalizeOperatorText(actor string) (string, error) {
	actor = strings.TrimSpace(strings.ToValidUTF8(actor, ""))
	if strings.ContainsRune(actor, '\x00') {
		return "", fmt.Errorf("%w: actor contains invalid control data", ErrInvalidInput)
	}
	return actor, nil
}

// safeStatusText collapses display-hostile whitespace and truncates by rune so
// status surfaces remain bounded without corrupting non-ASCII operator text.
func safeStatusText(value string, limit int) (string, bool) {
	value = strings.Join(strings.Fields(strings.ToValidUTF8(value, "")), " ")
	if limit <= 0 {
		return "", value != ""
	}
	if utf8.RuneCountInString(value) <= limit {
		return value, false
	}
	if limit <= utf8.RuneCountInString(redactionEllipsis) {
		return redactionEllipsis[:limit], true
	}

	runes := []rune(value)
	return string(runes[:limit-utf8.RuneCountInString(redactionEllipsis)]) + redactionEllipsis, true
}

// normalizeTime stores timestamps in UTC with monotonic clock data stripped so
// in-memory behavior matches the shape PostgreSQL persistence will round-trip.
func normalizeTime(t time.Time) time.Time {
	if t.IsZero() {
		return time.Time{}
	}
	return t.UTC().Round(0)
}

// cloneState copies pointer fields so callers cannot mutate store-owned
// expiration timestamps after a read, enable, or check operation.
func cloneState(state State) State {
	state.CreatedAt = normalizeTime(state.CreatedAt)
	state.ExpiresAt = cloneTime(state.ExpiresAt)
	return state
}

// cloneTime returns a deep copy of optional timestamps used by the state model.
func cloneTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	copied := normalizeTime(*t)
	return &copied
}
