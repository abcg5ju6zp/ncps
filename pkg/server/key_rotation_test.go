package server_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nix-community/go-nix/pkg/narinfo/signature"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	locklocal "github.com/kalbasit/ncps/pkg/lock/local"

	"github.com/kalbasit/ncps/pkg/cache"
	"github.com/kalbasit/ncps/pkg/database"
	"github.com/kalbasit/ncps/pkg/server"
	"github.com/kalbasit/ncps/pkg/storage/local"
	"github.com/kalbasit/ncps/testhelper"
)

func writeServerKeyFile(t *testing.T) string {
	t.Helper()

	sk, _, err := signature.GenerateKeypair(cacheName, nil)
	require.NoError(t, err)

	f, err := os.CreateTemp("", "server-secret-key-*")
	require.NoError(t, err)

	_, err = f.WriteString(sk.String())
	require.NoError(t, err)
	require.NoError(t, f.Close())

	t.Cleanup(func() { os.Remove(f.Name()) })

	return f.Name()
}

func newKeyFileCache(t *testing.T, dir, dbFile, keyPath string) (*cache.Cache, error) {
	t.Helper()

	dbClient, err := database.Open("sqlite:"+dbFile, nil)
	if err != nil {
		return nil, err
	}

	localStore, err := local.New(newContext(), dir)
	require.NoError(t, err)

	downloadLocker := locklocal.NewLocker()
	cacheLocker := locklocal.NewRWLocker()

	c, err := cache.New(newContext(), cacheName, dbClient, localStore, localStore, localStore, keyPath,
		downloadLocker, cacheLocker, 0, 0, 0)
	if err != nil {
		return nil, err
	}

	t.Cleanup(c.Close)
	t.Cleanup(func() { _ = dbClient.Close() })

	return c, nil
}

// TestPublicKeyRotationPublication verifies /pubkey and / publish both keys
// during a rotation and a single key when idle, with byte-stable idle
// output.
func TestPublicKeyRotationPublication(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "pubkey-rotation-")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })

	dbFile := filepath.Join(dir, "var", "ncps", "db", "db.sqlite")
	testhelper.CreateMigrateDatabase(t, dbFile)

	oldPath := writeServerKeyFile(t)
	newPath := writeServerKeyFile(t)

	cOld, err := newKeyFileCache(t, dir, dbFile, oldPath)
	require.NoError(t, err)

	oldPub := cOld.PublicKey().String()

	cNew, err := newKeyFileCache(t, dir, dbFile, newPath)
	require.NoError(t, err)
	require.NoError(t, cNew.ActivateKeyRotation(newContext()))

	newPub := cNew.PublicKey().String()

	s := server.New(cNew)
	ts := httptest.NewServer(s)
	t.Cleanup(ts.Close)

	doGet := func(baseURL string) *http.Response {
		t.Helper()

		req, err := http.NewRequestWithContext(newContext(), http.MethodGet, baseURL, nil)
		require.NoError(t, err)

		resp, err := ts.Client().Do(req)
		require.NoError(t, err)

		return resp
	}

	t.Run("pubkey lists both trusted keys", func(t *testing.T) {
		t.Parallel()

		resp := doGet(ts.URL + "/pubkey")
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, newPub+"\n"+oldPub, string(body))
	})

	t.Run("index exposes current key and the full key set", func(t *testing.T) {
		t.Parallel()

		resp := doGet(ts.URL + "/")
		defer resp.Body.Close()

		var body struct {
			Hostname   string   `json:"hostname"`
			PublicKey  string   `json:"publicKey"`
			PublicKeys []string `json:"publicKeys"`
		}

		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		assert.Equal(t, cacheName, body.Hostname)
		assert.Equal(t, newPub, body.PublicKey)
		assert.Equal(t, []string{newPub, oldPub}, body.PublicKeys)
	})

	t.Run("idle output is the single key with no extra newline", func(t *testing.T) {
		t.Parallel()

		idleDir, err := os.MkdirTemp("", "pubkey-idle-")
		require.NoError(t, err)
		t.Cleanup(func() { os.RemoveAll(idleDir) })

		idleDB := filepath.Join(idleDir, "var", "ncps", "db", "db.sqlite")
		testhelper.CreateMigrateDatabase(t, idleDB)

		cIdle, err := newKeyFileCache(t, idleDir, idleDB, writeServerKeyFile(t))
		require.NoError(t, err)
		require.NoError(t, cIdle.ActivateKeyRotation(newContext()))

		idleTS := httptest.NewServer(server.New(cIdle))
		t.Cleanup(idleTS.Close)

		req, err := http.NewRequestWithContext(newContext(), http.MethodGet, idleTS.URL+"/pubkey", nil)
		require.NoError(t, err)

		resp, err := idleTS.Client().Do(req)
		require.NoError(t, err)

		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, cIdle.PublicKey().String(), string(body))
		assert.False(t, strings.HasSuffix(string(body), "\n"))
	})
}
