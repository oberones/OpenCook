package maintenance

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestMemoryStoreDefaultsToDisabled(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	store := NewMemoryStore(WithClock(func() time.Time { return now }))

	state, err := store.Read(context.Background())
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if state.Enabled {
		t.Fatalf("Read().Enabled = true, want default disabled state")
	}

	check, err := store.Check(context.Background())
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if check.Active || check.Expired {
		t.Fatalf("Check() = active=%v expired=%v, want inactive and unexpired", check.Active, check.Expired)
	}
	if !check.CheckedAt.Equal(now) {
		t.Fatalf("Check().CheckedAt = %s, want %s", check.CheckedAt, now)
	}
}

func TestMemoryStoreEnableNormalizesInput(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 123, time.FixedZone("EDT", -4*60*60))
	expiresAt := now.Add(2 * time.Hour)
	store := NewMemoryStore(WithClock(func() time.Time { return now }))

	state, err := store.Enable(context.Background(), EnableInput{
		Mode:      " Source Sync ",
		Reason:    "  logical backup before cutover  ",
		Actor:     "  ops@example.test  ",
		ExpiresAt: &expiresAt,
	})
	if err != nil {
		t.Fatalf("Enable() error = %v", err)
	}
	if !state.Enabled {
		t.Fatalf("Enable().Enabled = false, want true")
	}
	if state.Mode != "source_sync" {
		t.Fatalf("Enable().Mode = %q, want source_sync", state.Mode)
	}
	if state.Reason != "logical backup before cutover" {
		t.Fatalf("Enable().Reason = %q, want trimmed reason", state.Reason)
	}
	if state.Actor != "ops@example.test" {
		t.Fatalf("Enable().Actor = %q, want trimmed actor", state.Actor)
	}
	if !state.CreatedAt.Equal(now.UTC()) {
		t.Fatalf("Enable().CreatedAt = %s, want UTC fallback %s", state.CreatedAt, now.UTC())
	}
	if state.ExpiresAt == nil || !state.ExpiresAt.Equal(expiresAt.UTC()) {
		t.Fatalf("Enable().ExpiresAt = %v, want UTC expiration %s", state.ExpiresAt, expiresAt.UTC())
	}
}

func TestMemoryStoreEnableRejectsInvalidInputWithoutMutation(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	store := NewMemoryStore(WithClock(func() time.Time { return now }))
	previous, err := store.Enable(context.Background(), EnableInput{Reason: "initial window"})
	if err != nil {
		t.Fatalf("Enable(initial) error = %v", err)
	}

	for _, tt := range []struct {
		name  string
		input EnableInput
	}{
		{name: "empty reason", input: EnableInput{Reason: "   "}},
		{name: "invalid reason control data", input: EnableInput{Reason: "bad\x00reason"}},
		{name: "invalid mode", input: EnableInput{Mode: "backup/restore", Reason: "bad mode"}},
		{name: "expired immediately", input: EnableInput{Reason: "bad expiration", CreatedAt: now, ExpiresAt: timePtr(now)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := store.Enable(context.Background(), tt.input); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("Enable() error = %v, want %v", err, ErrInvalidInput)
			}
			state, err := store.Read(context.Background())
			if err != nil {
				t.Fatalf("Read() error = %v", err)
			}
			assertStateEqual(t, state, previous)
		})
	}
}

func TestMemoryStoreExpirationDoesNotClearStoredState(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	expiresAt := now.Add(time.Minute)
	store := NewMemoryStore(WithClock(func() time.Time { return now }))

	if _, err := store.Enable(context.Background(), EnableInput{Reason: "short window", ExpiresAt: &expiresAt}); err != nil {
		t.Fatalf("Enable() error = %v", err)
	}
	check, err := store.Check(context.Background())
	if err != nil {
		t.Fatalf("Check(active) error = %v", err)
	}
	if !check.Active || check.Expired {
		t.Fatalf("Check(active) = active=%v expired=%v, want active and unexpired", check.Active, check.Expired)
	}

	now = expiresAt
	check, err = store.Check(context.Background())
	if err != nil {
		t.Fatalf("Check(expired) error = %v", err)
	}
	if check.Active || !check.Expired {
		t.Fatalf("Check(expired) = active=%v expired=%v, want inactive and expired", check.Active, check.Expired)
	}
	state, err := store.Read(context.Background())
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !state.Enabled || state.ExpiresAt == nil {
		t.Fatalf("Read() after expiration = %+v, want stored enabled state preserved", state)
	}
}

func TestMemoryStoreDisableIsIdempotentAndClearsState(t *testing.T) {
	store := NewMemoryStore()
	if _, err := store.Enable(context.Background(), EnableInput{Reason: "operator window"}); err != nil {
		t.Fatalf("Enable() error = %v", err)
	}

	for i := 0; i < 2; i++ {
		state, err := store.Disable(context.Background(), DisableInput{Actor: "operator"})
		if err != nil {
			t.Fatalf("Disable(%d) error = %v", i, err)
		}
		if state.Enabled {
			t.Fatalf("Disable(%d).Enabled = true, want false", i)
		}
	}
	check, err := store.Check(context.Background())
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if check.Active || check.State.Enabled {
		t.Fatalf("Check() after disable = active=%v state=%+v, want disabled", check.Active, check.State)
	}
}

func TestMemoryStoreCopiesExpirationTimestamps(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	expiresAt := now.Add(time.Hour)
	store := NewMemoryStore(WithClock(func() time.Time { return now }))

	state, err := store.Enable(context.Background(), EnableInput{Reason: "copy expiration", ExpiresAt: &expiresAt})
	if err != nil {
		t.Fatalf("Enable() error = %v", err)
	}
	*state.ExpiresAt = now.Add(-time.Hour)

	check, err := store.Check(context.Background())
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if !check.Active {
		t.Fatalf("Check().Active = false, want store-owned expiration protected from caller mutation")
	}
}

func TestStateSafeStatusBoundsOperatorText(t *testing.T) {
	state := State{
		Enabled:   true,
		Mode:      "cutover",
		Reason:    "rotate\n" + strings.Repeat("x", reasonStatusRuneLimit+20),
		Actor:     "ops\t" + strings.Repeat("y", actorStatusRuneLimit+20),
		CreatedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.FixedZone("EDT", -4*60*60)),
		ExpiresAt: timePtr(time.Date(2026, 5, 1, 14, 0, 0, 0, time.FixedZone("EDT", -4*60*60))),
	}

	safe := state.SafeStatus()
	if strings.ContainsAny(safe.Reason, "\n\t") {
		t.Fatalf("SafeStatus().Reason = %q, want collapsed whitespace", safe.Reason)
	}
	if strings.ContainsAny(safe.Actor, "\n\t") {
		t.Fatalf("SafeStatus().Actor = %q, want collapsed whitespace", safe.Actor)
	}
	if !safe.ReasonTruncated || len([]rune(safe.Reason)) > reasonStatusRuneLimit {
		t.Fatalf("SafeStatus().Reason = %q truncated=%v, want bounded truncated reason", safe.Reason, safe.ReasonTruncated)
	}
	if !safe.ActorTruncated || len([]rune(safe.Actor)) > actorStatusRuneLimit {
		t.Fatalf("SafeStatus().Actor = %q truncated=%v, want bounded truncated actor", safe.Actor, safe.ActorTruncated)
	}
	if safe.CreatedAt.Location() != time.UTC {
		t.Fatalf("SafeStatus().CreatedAt location = %s, want UTC", safe.CreatedAt.Location())
	}
	if safe.ExpiresAt == nil || safe.ExpiresAt.Location() != time.UTC {
		t.Fatalf("SafeStatus().ExpiresAt = %v, want UTC timestamp", safe.ExpiresAt)
	}
}

func TestMemoryStoreRespectsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := NewMemoryStore()

	if _, err := store.Read(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Read(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := store.Enable(ctx, EnableInput{Reason: "no-op"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Enable(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := store.Disable(ctx, DisableInput{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Disable(canceled) error = %v, want context.Canceled", err)
	}
	if _, err := store.Check(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Check(canceled) error = %v, want context.Canceled", err)
	}
}

// assertStateEqual compares maintenance state values while handling optional
// expiration pointers without exposing pointer identity to the tests.
func assertStateEqual(t *testing.T, got, want State) {
	t.Helper()
	if got.Enabled != want.Enabled || got.Mode != want.Mode || got.Reason != want.Reason || got.Actor != want.Actor || !got.CreatedAt.Equal(want.CreatedAt) {
		t.Fatalf("state = %+v, want %+v", got, want)
	}
	switch {
	case got.ExpiresAt == nil && want.ExpiresAt == nil:
	case got.ExpiresAt != nil && want.ExpiresAt != nil && got.ExpiresAt.Equal(*want.ExpiresAt):
	default:
		t.Fatalf("state expiration = %v, want %v", got.ExpiresAt, want.ExpiresAt)
	}
}

// timePtr keeps table tests readable when they need an optional expiration.
func timePtr(t time.Time) *time.Time {
	return &t
}
