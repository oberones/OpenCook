package authz

import (
	"context"
	"errors"
)

var ErrNotImplemented = errors.New("chef authorization not implemented")

type Action string

const (
	ActionRead   Action = "read"
	ActionCreate Action = "create"
	ActionUpdate Action = "update"
	ActionDelete Action = "delete"
)

type Subject struct {
	Type         string `json:"type"`
	Name         string `json:"name"`
	Organization string `json:"organization,omitempty"`
}

type Resource struct {
	Type         string `json:"type"`
	Name         string `json:"name"`
	Organization string `json:"organization,omitempty"`
}

type Decision struct {
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason"`
}

type Authorizer interface {
	Name() string
	Authorize(context.Context, Subject, Action, Resource) (Decision, error)
}

type NoopAuthorizer struct{}

func (NoopAuthorizer) Name() string {
	return "noop-bifrost"
}

func (NoopAuthorizer) Authorize(_ context.Context, _ Subject, _ Action, _ Resource) (Decision, error) {
	return Decision{
		Allowed: false,
		Reason:  "authorization scaffold only",
	}, ErrNotImplemented
}

