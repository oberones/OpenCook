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
	ActionGrant  Action = "grant"
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

type Permission struct {
	Actors []string `json:"actors"`
	Groups []string `json:"groups"`
}

type ACL struct {
	Create Permission `json:"create"`
	Read   Permission `json:"read"`
	Update Permission `json:"update"`
	Delete Permission `json:"delete"`
	Grant  Permission `json:"grant"`
}

type Authorizer interface {
	Name() string
	Authorize(context.Context, Subject, Action, Resource) (Decision, error)
}

type ACLResolver interface {
	ResolveACL(context.Context, Resource) (ACL, bool, error)
	GroupsFor(context.Context, Subject) ([]string, error)
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

type ACLAuthorizer struct {
	resolver ACLResolver
}

func NewACLAuthorizer(resolver ACLResolver) ACLAuthorizer {
	return ACLAuthorizer{resolver: resolver}
}

func (ACLAuthorizer) Name() string {
	return "memory-acl-bifrost"
}

func (a ACLAuthorizer) Authorize(ctx context.Context, subject Subject, action Action, resource Resource) (Decision, error) {
	if a.resolver == nil {
		return Decision{
			Allowed: false,
			Reason:  "authorization resolver missing",
		}, ErrNotImplemented
	}

	acl, ok, err := a.resolver.ResolveACL(ctx, resource)
	if err != nil {
		return Decision{}, err
	}
	if !ok {
		return Decision{
			Allowed: false,
			Reason:  "resource ACL not found",
		}, nil
	}

	permission := aclPermission(acl, action)
	if contains(permission.Actors, subject.Name) {
		return Decision{
			Allowed: true,
			Reason:  "actor explicitly allowed",
		}, nil
	}

	groups, err := a.resolver.GroupsFor(ctx, subject)
	if err != nil {
		return Decision{}, err
	}
	if intersects(groups, permission.Groups) {
		return Decision{
			Allowed: true,
			Reason:  "group membership allowed",
		}, nil
	}

	return Decision{
		Allowed: false,
		Reason:  "acl denied",
	}, nil
}

func aclPermission(acl ACL, action Action) Permission {
	switch action {
	case ActionCreate:
		return acl.Create
	case ActionRead:
		return acl.Read
	case ActionUpdate:
		return acl.Update
	case ActionDelete:
		return acl.Delete
	case ActionGrant:
		return acl.Grant
	default:
		return Permission{}
	}
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func intersects(left, right []string) bool {
	for _, candidate := range left {
		if contains(right, candidate) {
			return true
		}
	}
	return false
}
