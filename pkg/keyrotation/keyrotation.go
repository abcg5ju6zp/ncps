// Package keyrotation implements the signing-key generation rotation state
// machine for the cache. A configuration key change is treated as a
// recoverable generation: the previous key stays trusted while every cached
// narinfo is re-signed with the current key, and the current/previous keys,
// phase, and re-sign cursor are persisted so restarts and concurrent replicas
// always resume the same generation.
package keyrotation

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nix-community/go-nix/pkg/narinfo/signature"
)

// Phase is the persisted rotation phase.
type Phase string

const (
	// PhaseRotating is the only persisted phase: dual-key publication and
	// lazy/background re-signing. Completion is represented by deleting the
	// state row (and previous secret) rather than by a terminal phase.
	PhaseRotating Phase = "rotating"
)

// Role identifies which generation key a joined node holds.
type Role string

const (
	// RoleNone means no rotation is active on the node.
	RoleNone Role = ""
	// RoleCurrent means the node holds the current key: it signs new
	// writes, runs the re-sign worker, and can complete the rotation.
	RoleCurrent Role = "current"
	// RolePrevious means a staggered-upgrade node still holds the previous
	// key: it serves and fills old signatures but cannot re-sign or retire
	// the generation.
	RolePrevious Role = "previous"
)

var (
	// ErrRetiredSigningKey is returned when the node is configured with a
	// secret key whose public key was retired by a completed rotation.
	ErrRetiredSigningKey = errors.New("configured signing key has been retired")

	// ErrConcurrentKeyRotation is returned when a rotation is in progress
	// but the configured key matches neither the current nor the previous
	// generation, so starting a second generation is refused.
	ErrConcurrentKeyRotation = errors.New("a signing-key rotation is already in progress with a different key")

	// ErrIdenticalKeys is returned when a rotation is started with current
	// and previous secrets deriving the same public key.
	ErrIdenticalKeys = errors.New("cannot begin a key rotation: current and previous public keys are identical")

	// ErrUnknownPhase is returned when a persisted state carries an
	// unrecognized phase.
	ErrUnknownPhase = errors.New("unknown key rotation phase")

	// ErrPublicKeyMismatch is returned when a persisted public key does not
	// derive from its persisted secret key.
	ErrPublicKeyMismatch = errors.New("persisted public key does not match the secret key")
)

// State is the JSON document persisted under config key
// "key_rotation_state".
type State struct {
	// Generation is the monotonically increasing rotation number.
	Generation int `json:"generation"`
	// Phase is always PhaseRotating for a persisted document.
	Phase Phase `json:"phase"`
	// CurrentSecretKey and CurrentPublicKey are the active signing
	// generation ("name:base64" nix format).
	CurrentSecretKey string `json:"currentSecretKey"`
	CurrentPublicKey string `json:"currentPublicKey"`
	// PreviousSecretKey and PreviousPublicKey are retained until the
	// coverage-gated retirement deletes the document.
	PreviousSecretKey string `json:"previousSecretKey"`
	PreviousPublicKey string `json:"previousPublicKey"`
	// ResignCursor is the hash of the last fully processed narinfo; ""
	// means the sweep starts at the beginning (also during the
	// confirmation sweep).
	ResignCursor string `json:"resignCursor"`
	// Confirming marks the end-to-end zero-pending confirmation sweep
	// that gates retirement.
	Confirming bool `json:"confirming"`
}

// NewState builds the initial rotating state from the current (new) secret
// and the previous (old) secret, deriving the recorded public keys.
func NewState(generation int, currentSecret, previousSecret string) (*State, error) {
	current, err := signature.LoadSecretKey(currentSecret)
	if err != nil {
		return nil, fmt.Errorf("error loading the current secret key: %w", err)
	}

	previous, err := signature.LoadSecretKey(previousSecret)
	if err != nil {
		return nil, fmt.Errorf("error loading the previous secret key: %w", err)
	}

	if current.ToPublicKey().String() == previous.ToPublicKey().String() {
		return nil, ErrIdenticalKeys
	}

	return &State{
		Generation:        generation,
		Phase:             PhaseRotating,
		CurrentSecretKey:  currentSecret,
		CurrentPublicKey:  current.ToPublicKey().String(),
		PreviousSecretKey: previousSecret,
		PreviousPublicKey: previous.ToPublicKey().String(),
	}, nil
}

// EncodeState serializes the state as a JSON document.
func EncodeState(s *State) (string, error) {
	raw, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("error encoding key rotation state: %w", err)
	}

	return string(raw), nil
}

// DecodeState parses and validates a persisted state document. It rejects
// unknown phases, malformed keys, and public keys that do not derive from
// the recorded secrets.
func DecodeState(raw string) (*State, error) {
	var s State

	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		return nil, fmt.Errorf("error decoding key rotation state: %w", err)
	}

	if s.Phase != PhaseRotating {
		return nil, fmt.Errorf("%w: %q", ErrUnknownPhase, s.Phase)
	}

	current, err := signature.LoadSecretKey(s.CurrentSecretKey)
	if err != nil {
		return nil, fmt.Errorf("error loading persisted current secret key: %w", err)
	}

	if s.CurrentPublicKey != current.ToPublicKey().String() {
		return nil, fmt.Errorf("current generation: %w", ErrPublicKeyMismatch)
	}

	previous, err := signature.LoadSecretKey(s.PreviousSecretKey)
	if err != nil {
		return nil, fmt.Errorf("error loading persisted previous secret key: %w", err)
	}

	if s.PreviousPublicKey != previous.ToPublicKey().String() {
		return nil, fmt.Errorf("previous generation: %w", ErrPublicKeyMismatch)
	}

	return &s, nil
}

// AdvanceCursor records that the narinfo with hash has been fully processed.
func (s *State) AdvanceCursor(hash string) {
	s.ResignCursor = hash
}

// ArmConfirmationSweep restarts the walk from the beginning in confirming
// mode: a complete sweep with zero modifications and zero unverifiable
// objects is the coverage condition for retirement.
func (s *State) ArmConfirmationSweep() {
	s.Confirming = true
	s.ResignCursor = ""
}

// HasValidSignature reports whether at least one signature validates
// against one of the public keys. Unlike signature.VerifyFirst it keeps
// scanning same-name signatures, which matters during a rotation because the
// old and new keys share the same key name and a narinfo can carry both.
func HasValidSignature(fingerprint string, sigs []signature.Signature, pubs []signature.PublicKey) bool {
	for _, pub := range pubs {
		for _, sig := range sigs {
			if pub.Name == sig.Name && pub.Verify(fingerprint, sig) {
				return true
			}
		}
	}

	return false
}

// Action is the startup decision produced by Inspect.
type Action int

const (
	// ActionIdle: no rotation to enter or join.
	ActionIdle Action = iota
	// ActionBegin: persist a new rotating generation for a changed key.
	ActionBegin
	// ActionJoin: resume the persisted generation in the returned Role.
	ActionJoin
)

// InspectInput carries the startup key observations.
type InspectInput struct {
	// StoredState is the persisted rotation document, or nil.
	StoredState *State
	// FilePublicKey is the public key derived from the configured key
	// file; "" when no key file is configured (database-managed key).
	FilePublicKey string
	// StoredPublicKey is the public key of the secret_key config row.
	StoredPublicKey string
	// RetiredPublicKeys lists public keys retired by completed rotations.
	RetiredPublicKeys []string
}

// InspectResult is the decision for startup.
type InspectResult struct {
	Action Action
	Role   Role
	State  *State
}

// Inspect decides how startup must reconcile the configured key with the
// persisted generation. It is a pure function: callers perform the resulting
// persistence and key activation.
func Inspect(in InspectInput) (*InspectResult, error) {
	retired := make(map[string]struct{}, len(in.RetiredPublicKeys))
	for _, k := range in.RetiredPublicKeys {
		retired[k] = struct{}{}
	}

	if in.StoredState != nil {
		// A node without a key file resumes from the stored secret; a node
		// with a file is bound to its file key.
		nodeKey := in.FilePublicKey
		if nodeKey == "" {
			nodeKey = in.StoredPublicKey
		}

		switch nodeKey {
		case in.StoredState.CurrentPublicKey:
			return &InspectResult{Action: ActionJoin, Role: RoleCurrent, State: in.StoredState}, nil
		case in.StoredState.PreviousPublicKey:
			return &InspectResult{Action: ActionJoin, Role: RolePrevious, State: in.StoredState}, nil
		}

		if _, ok := retired[nodeKey]; ok {
			return nil, ErrRetiredSigningKey
		}

		return nil, ErrConcurrentKeyRotation
	}

	if in.FilePublicKey == "" {
		return &InspectResult{Action: ActionIdle}, nil
	}

	// A retired key is refused even if it still matches the stored secret
	// (e.g. a stale deployment whose secret_key row was never advanced).
	if _, ok := retired[in.FilePublicKey]; ok {
		return nil, ErrRetiredSigningKey
	}

	if in.FilePublicKey == in.StoredPublicKey {
		return &InspectResult{Action: ActionIdle}, nil
	}

	return &InspectResult{Action: ActionBegin, Role: RoleCurrent}, nil
}
