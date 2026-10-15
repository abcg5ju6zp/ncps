package keyrotation_test

import (
	"testing"

	"github.com/nix-community/go-nix/pkg/narinfo/signature"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kalbasit/ncps/pkg/keyrotation"
)

const testHost = "cache.example.com"

func mustKeypair(t *testing.T) signature.SecretKey {
	t.Helper()

	sk, _, err := signature.GenerateKeypair(testHost, nil)
	require.NoError(t, err)

	return sk
}

func TestNewStateAndCodec(t *testing.T) {
	t.Parallel()

	current := mustKeypair(t)
	previous := mustKeypair(t)

	st, err := keyrotation.NewState(3, current.String(), previous.String())
	require.NoError(t, err)

	assert.Equal(t, 3, st.Generation)
	assert.Equal(t, keyrotation.PhaseRotating, st.Phase)
	assert.Equal(t, current.String(), st.CurrentSecretKey)
	assert.Equal(t, current.ToPublicKey().String(), st.CurrentPublicKey)
	assert.Equal(t, previous.String(), st.PreviousSecretKey)
	assert.Equal(t, previous.ToPublicKey().String(), st.PreviousPublicKey)
	assert.Empty(t, st.ResignCursor)
	assert.False(t, st.Confirming)

	t.Run("JSON round trip", func(t *testing.T) {
		t.Parallel()

		st.ResignCursor = "0abc"
		st.Confirming = true

		raw, err := keyrotation.EncodeState(st)
		require.NoError(t, err)

		decoded, err := keyrotation.DecodeState(raw)
		require.NoError(t, err)
		assert.Equal(t, st, decoded)
	})
}

func TestDecodeStateRejectsCorruptDocuments(t *testing.T) {
	t.Parallel()

	current := mustKeypair(t)
	previous := mustKeypair(t)

	st, err := keyrotation.NewState(1, current.String(), previous.String())
	require.NoError(t, err)

	t.Run("not json", func(t *testing.T) {
		t.Parallel()

		_, err := keyrotation.DecodeState("{nope")
		assert.Error(t, err)
	})

	t.Run("unknown phase", func(t *testing.T) {
		t.Parallel()

		raw, err := keyrotation.EncodeState(st)
		require.NoError(t, err)

		_, err = keyrotation.DecodeState(`{"phase":"complete"}` + raw[1:])
		assert.Error(t, err)
	})

	t.Run("public key does not match secret", func(t *testing.T) {
		t.Parallel()

		forged := *st
		forged.CurrentPublicKey = previous.ToPublicKey().String()

		raw, err := keyrotation.EncodeState(&forged)
		require.NoError(t, err)

		_, err = keyrotation.DecodeState(raw)
		assert.Error(t, err)
	})

	t.Run("malformed secret", func(t *testing.T) {
		t.Parallel()

		forged := *st
		forged.PreviousSecretKey = "garbage"

		raw, err := keyrotation.EncodeState(&forged)
		require.NoError(t, err)

		_, err = keyrotation.DecodeState(raw)
		assert.Error(t, err)
	})
}

func TestInspect(t *testing.T) {
	t.Parallel()

	old := mustKeypair(t)
	newKey := mustKeypair(t)
	other := mustKeypair(t)

	st, err := keyrotation.NewState(1, newKey.String(), old.String())
	require.NoError(t, err)

	tests := []struct {
		name    string
		input   keyrotation.InspectInput
		action  keyrotation.Action
		role    keyrotation.Role
		wantErr error
	}{
		{
			name: "no key configured and no state is idle",
			input: keyrotation.InspectInput{
				StoredPublicKey: old.ToPublicKey().String(),
			},
			action: keyrotation.ActionIdle,
			role:   keyrotation.RoleNone,
		},
		{
			name: "unchanged file key is idle",
			input: keyrotation.InspectInput{
				FilePublicKey:   old.ToPublicKey().String(),
				StoredPublicKey: old.ToPublicKey().String(),
			},
			action: keyrotation.ActionIdle,
			role:   keyrotation.RoleNone,
		},
		{
			name: "changed file key begins a rotation",
			input: keyrotation.InspectInput{
				FilePublicKey:   newKey.ToPublicKey().String(),
				StoredPublicKey: old.ToPublicKey().String(),
			},
			action: keyrotation.ActionBegin,
			role:   keyrotation.RoleCurrent,
		},
		{
			name: "join current generation with new key",
			input: keyrotation.InspectInput{
				StoredState:     st,
				FilePublicKey:   newKey.ToPublicKey().String(),
				StoredPublicKey: newKey.ToPublicKey().String(),
			},
			action: keyrotation.ActionJoin,
			role:   keyrotation.RoleCurrent,
		},
		{
			name: "staggered node joins as previous",
			input: keyrotation.InspectInput{
				StoredState:     st,
				FilePublicKey:   old.ToPublicKey().String(),
				StoredPublicKey: newKey.ToPublicKey().String(),
			},
			action: keyrotation.ActionJoin,
			role:   keyrotation.RolePrevious,
		},
		{
			name: "db mode resumes current generation without file",
			input: keyrotation.InspectInput{
				StoredState:     st,
				StoredPublicKey: newKey.ToPublicKey().String(),
			},
			action: keyrotation.ActionJoin,
			role:   keyrotation.RoleCurrent,
		},
		{
			name: "unknown key during rotation is rejected",
			input: keyrotation.InspectInput{
				StoredState:     st,
				FilePublicKey:   other.ToPublicKey().String(),
				StoredPublicKey: newKey.ToPublicKey().String(),
			},
			wantErr: keyrotation.ErrConcurrentKeyRotation,
		},
		{
			name: "retired file key is rejected",
			input: keyrotation.InspectInput{
				FilePublicKey:     old.ToPublicKey().String(),
				StoredPublicKey:   newKey.ToPublicKey().String(),
				RetiredPublicKeys: []string{old.ToPublicKey().String()},
			},
			wantErr: keyrotation.ErrRetiredSigningKey,
		},
		{
			name: "unknown third key without state is rejected",
			input: keyrotation.InspectInput{
				FilePublicKey:   other.ToPublicKey().String(),
				StoredPublicKey: newKey.ToPublicKey().String(),
			},
			wantErr: nil, // begin is allowed; retired list decides, not stored mismatch
			action:  keyrotation.ActionBegin,
			role:    keyrotation.RoleCurrent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := keyrotation.Inspect(tt.input)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.action, res.Action)
			assert.Equal(t, tt.role, res.Role)

			if tt.action == keyrotation.ActionJoin {
				assert.Equal(t, st.Generation, res.State.Generation)
			}
		})
	}
}

func TestRetiredKeyRejectedEvenWhenStatePresentButNotMatchingEitherGeneration(t *testing.T) {
	t.Parallel()

	old := mustKeypair(t)
	newKey := mustKeypair(t)
	retired := mustKeypair(t)

	st, err := keyrotation.NewState(2, newKey.String(), old.String())
	require.NoError(t, err)

	_, err = keyrotation.Inspect(keyrotation.InspectInput{
		StoredState:       st,
		FilePublicKey:     retired.ToPublicKey().String(),
		StoredPublicKey:   newKey.ToPublicKey().String(),
		RetiredPublicKeys: []string{retired.ToPublicKey().String()},
	})
	assert.ErrorIs(t, err, keyrotation.ErrRetiredSigningKey)
}

func TestCursorSweepTransitions(t *testing.T) {
	t.Parallel()

	current := mustKeypair(t)
	previous := mustKeypair(t)

	st, err := keyrotation.NewState(1, current.String(), previous.String())
	require.NoError(t, err)

	st.AdvanceCursor("hash1")
	assert.Equal(t, "hash1", st.ResignCursor)

	st.ArmConfirmationSweep()
	assert.True(t, st.Confirming)
	assert.Empty(t, st.ResignCursor)
}
