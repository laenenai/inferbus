// Package org implements the "org" aggregate for the inferbus control
// plane: an organization's members (OIDC sub -> owner|admin|viewer) and its
// projects. It is an es-lite Decider hand-written against the
// controlplane.v1 proto types generated in Task 1 (see
// api/controlplane/v1/org.proto): OrgCommand and OrgEvent are `oneof kind`
// wrapper messages rather than es-lite's own sealed-interface codegen
// idiom (examples/counter), so Decide/Evolve switch on GetKind() and the
// codec (codec.go) maps each wrapped variant to its own TypeURL.
package org

import (
	"errors"
	"fmt"

	controlplanev1 "github.com/laenenai/inferbus/api/controlplane/v1"

	"github.com/laenenai/es-lite/es"
	"google.golang.org/protobuf/proto"
)

// StreamType is the es.StreamID.Type for every org stream.
const StreamType = "org"

// Domain errors, surfaced verbatim by aggregate.Runtime.Handle when Decide
// rejects a command.
var (
	// ErrAlreadyExists is returned by CreateOrg when the stream already has
	// an org (Id is non-empty).
	ErrAlreadyExists = errors.New("org: already exists")

	// ErrNotFound is returned by every command other than CreateOrg when
	// issued against a stream with no org yet (Id is empty).
	ErrNotFound = errors.New("org: not found")

	// ErrLastOwner is returned by RemoveMember when removing the given sub
	// would leave the org with zero owners.
	ErrLastOwner = errors.New("org: cannot remove the last owner")

	// ErrNoSuchProject is returned by ArchiveProject when no project with
	// the given id exists.
	ErrNoSuchProject = errors.New("org: no such project")

	// ErrInvalidRole is returned by UpsertMember when the role is not one
	// of owner, admin, or viewer.
	ErrInvalidRole = errors.New("org: invalid role")
)

// validRoles restricts UpsertMember.role (and the owner role assigned by
// CreateOrg) to the three roles the control plane understands.
var validRoles = map[string]bool{
	"owner":  true,
	"admin":  true,
	"viewer": true,
}

// Decider is the org aggregate's business logic. State is the Org proto
// message used directly; an empty Id means the stream has no org yet.
var Decider = es.Decider[*controlplanev1.Org, *controlplanev1.OrgCommand, *controlplanev1.OrgEvent]{
	Initial: func() *controlplanev1.Org { return &controlplanev1.Org{} },

	Decide: func(s *controlplanev1.Org, cmd *controlplanev1.OrgCommand) ([]*controlplanev1.OrgEvent, []es.ConstraintOp, error) {
		switch k := cmd.GetKind().(type) {

		case *controlplanev1.OrgCommand_Create:
			if s.GetId() != "" {
				return nil, nil, ErrAlreadyExists
			}
			c := k.Create
			return []*controlplanev1.OrgEvent{
				wrap(&controlplanev1.OrgCreated{
					Id:       c.GetId(),
					Name:     c.GetName(),
					OwnerSub: c.GetOwnerSub(),
				}),
			}, nil, nil

		case *controlplanev1.OrgCommand_Rename:
			if s.GetId() == "" {
				return nil, nil, ErrNotFound
			}
			return []*controlplanev1.OrgEvent{
				wrap(&controlplanev1.OrgRenamed{Name: k.Rename.GetName()}),
			}, nil, nil

		case *controlplanev1.OrgCommand_UpsertMember:
			if s.GetId() == "" {
				return nil, nil, ErrNotFound
			}
			role := k.UpsertMember.GetRole()
			if !validRoles[role] {
				return nil, nil, ErrInvalidRole
			}
			return []*controlplanev1.OrgEvent{
				wrap(&controlplanev1.MemberUpserted{
					Sub:  k.UpsertMember.GetSub(),
					Role: role,
				}),
			}, nil, nil

		case *controlplanev1.OrgCommand_RemoveMember:
			if s.GetId() == "" {
				return nil, nil, ErrNotFound
			}
			sub := k.RemoveMember.GetSub()
			if role, ok := s.GetMembers()[sub]; ok && role == "owner" && ownerCount(s) <= 1 {
				return nil, nil, ErrLastOwner
			}
			return []*controlplanev1.OrgEvent{
				wrap(&controlplanev1.MemberRemoved{Sub: sub}),
			}, nil, nil

		case *controlplanev1.OrgCommand_CreateProject:
			if s.GetId() == "" {
				return nil, nil, ErrNotFound
			}
			return []*controlplanev1.OrgEvent{
				wrap(&controlplanev1.ProjectCreated{
					Id:   k.CreateProject.GetId(),
					Name: k.CreateProject.GetName(),
				}),
			}, nil, nil

		case *controlplanev1.OrgCommand_ArchiveProject:
			if s.GetId() == "" {
				return nil, nil, ErrNotFound
			}
			id := k.ArchiveProject.GetId()
			if _, ok := s.GetProjects()[id]; !ok {
				return nil, nil, ErrNoSuchProject
			}
			return []*controlplanev1.OrgEvent{
				wrap(&controlplanev1.ProjectArchived{Id: id}),
			}, nil, nil

		default:
			return nil, nil, fmt.Errorf("org: unknown command kind %T", cmd.GetKind())
		}
	},

	Evolve: func(s *controlplanev1.Org, e *controlplanev1.OrgEvent) *controlplanev1.Org {
		ns, ok := proto.Clone(s).(*controlplanev1.Org)
		if !ok {
			// proto.Clone on an Org always yields an Org; unreachable.
			ns = &controlplanev1.Org{}
		}

		switch k := e.GetKind().(type) {

		case *controlplanev1.OrgEvent_Created:
			ns.Id = k.Created.GetId()
			ns.Name = k.Created.GetName()
			if ns.Members == nil {
				ns.Members = map[string]string{}
			}
			ns.Members[k.Created.GetOwnerSub()] = "owner"

		case *controlplanev1.OrgEvent_Renamed:
			ns.Name = k.Renamed.GetName()

		case *controlplanev1.OrgEvent_MemberUpserted:
			if ns.Members == nil {
				ns.Members = map[string]string{}
			}
			ns.Members[k.MemberUpserted.GetSub()] = k.MemberUpserted.GetRole()

		case *controlplanev1.OrgEvent_MemberRemoved:
			delete(ns.Members, k.MemberRemoved.GetSub())

		case *controlplanev1.OrgEvent_ProjectCreated:
			if ns.Projects == nil {
				ns.Projects = map[string]*controlplanev1.Project{}
			}
			ns.Projects[k.ProjectCreated.GetId()] = &controlplanev1.Project{
				Id:   k.ProjectCreated.GetId(),
				Name: k.ProjectCreated.GetName(),
			}

		case *controlplanev1.OrgEvent_ProjectArchived:
			if p, ok := ns.Projects[k.ProjectArchived.GetId()]; ok {
				p.Archived = true
			}
		}

		return ns
	},
}

// wrap lifts a concrete event payload into the OrgEvent oneof wrapper.
func wrap(payload any) *controlplanev1.OrgEvent {
	switch p := payload.(type) {
	case *controlplanev1.OrgCreated:
		return &controlplanev1.OrgEvent{Kind: &controlplanev1.OrgEvent_Created{Created: p}}
	case *controlplanev1.OrgRenamed:
		return &controlplanev1.OrgEvent{Kind: &controlplanev1.OrgEvent_Renamed{Renamed: p}}
	case *controlplanev1.MemberUpserted:
		return &controlplanev1.OrgEvent{Kind: &controlplanev1.OrgEvent_MemberUpserted{MemberUpserted: p}}
	case *controlplanev1.MemberRemoved:
		return &controlplanev1.OrgEvent{Kind: &controlplanev1.OrgEvent_MemberRemoved{MemberRemoved: p}}
	case *controlplanev1.ProjectCreated:
		return &controlplanev1.OrgEvent{Kind: &controlplanev1.OrgEvent_ProjectCreated{ProjectCreated: p}}
	case *controlplanev1.ProjectArchived:
		return &controlplanev1.OrgEvent{Kind: &controlplanev1.OrgEvent_ProjectArchived{ProjectArchived: p}}
	default:
		panic(fmt.Sprintf("org: wrap: unknown event payload %T", payload))
	}
}

// ownerCount returns how many members currently hold the "owner" role.
func ownerCount(s *controlplanev1.Org) int {
	n := 0
	for _, role := range s.GetMembers() {
		if role == "owner" {
			n++
		}
	}
	return n
}
