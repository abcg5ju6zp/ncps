package config_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kalbasit/ncps/pkg/config"
	"github.com/kalbasit/ncps/pkg/lock/local"
)

func TestKeyRotationStateBackends(t *testing.T) {
	t.Parallel()

	backends := []struct {
		name   string
		envVar string
		setup  databaseFactory
	}{
		{name: "SQLite", setup: setupSQLiteDatabase},
		{name: "PostgreSQL", envVar: "NCPS_TEST_ADMIN_POSTGRES_URL", setup: setupPostgresDatabase},
		{name: "MySQL", envVar: "NCPS_TEST_ADMIN_MYSQL_URL", setup: setupMySQLDatabase},
	}

	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			t.Parallel()

			if b.envVar != "" && os.Getenv(b.envVar) == "" {
				t.Skipf("Skipping %s: %s not set", b.name, b.envVar)
			}

			t.Run("missing state is not found", func(t *testing.T) {
				t.Parallel()

				db, cleanup := b.setup(t)
				t.Cleanup(cleanup)

				c := config.New(db, local.NewRWLocker())

				_, err := c.GetKeyRotationState(context.Background())
				require.ErrorIs(t, err, config.ErrConfigNotFound)
			})

			t.Run("state is stored read and deleted", func(t *testing.T) {
				t.Parallel()

				db, cleanup := b.setup(t)
				t.Cleanup(cleanup)

				c := config.New(db, local.NewRWLocker())

				require.NoError(t, c.SetKeyRotationState(context.Background(), `{"generation":2}`))

				got, err := c.GetKeyRotationState(context.Background())
				require.NoError(t, err)
				assert.Equal(t, `{"generation":2}`, got)

				require.NoError(t, c.DeleteKeyRotationState(context.Background()))

				_, err = c.GetKeyRotationState(context.Background())
				require.ErrorIs(t, err, config.ErrConfigNotFound)

				// Deleting a missing row is not an error so concurrent
				// retirement by two replicas cannot make the second fail.
				assert.NoError(t, c.DeleteKeyRotationState(context.Background()))
			})
		})
	}
}

func TestRetiredPublicKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("no retired keys before any rotation", func(t *testing.T) {
		t.Parallel()

		db, cleanup := setupSQLiteDatabase(t)
		t.Cleanup(cleanup)

		c := config.New(db, local.NewRWLocker())

		keys, err := c.GetRetiredPublicKeys(ctx)
		require.NoError(t, err)
		assert.Empty(t, keys)
	})

	t.Run("keys are appended once and de-duplicated", func(t *testing.T) {
		t.Parallel()

		db, cleanup := setupSQLiteDatabase(t)
		t.Cleanup(cleanup)

		c := config.New(db, local.NewRWLocker())

		require.NoError(t, c.AddRetiredPublicKey(ctx, "cache.example.com:aaaa"))
		require.NoError(t, c.AddRetiredPublicKey(ctx, "cache.example.com:bbbb"))
		require.NoError(t, c.AddRetiredPublicKey(ctx, "cache.example.com:aaaa"))

		keys, err := c.GetRetiredPublicKeys(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"cache.example.com:aaaa", "cache.example.com:bbbb"}, keys)
	})
}
