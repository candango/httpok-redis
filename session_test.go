package redisstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/candango/httpok/session"
	"github.com/redis/go-redis/v9"
)

func newTestStore(t *testing.T, options ...Option) (*Store, *miniredis.Miniredis) {
	t.Helper()

	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	options = append(options, WithClient(client))
	store, err := New(options...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := store.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := store.Stop(context.Background()); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
		_ = client.Close()
	})
	return store, server
}

func TestStoreImplementsSessionStore(t *testing.T) {
	var _ session.Store = (*Store)(nil)
	var _ interface {
		Read(string, any) ([]byte, error)
	} = (*Store)(nil)
}

func TestNewValidatesConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		options []Option
	}{
		{name: "empty address", options: []Option{WithAddress("")}},
		{name: "empty prefix", options: []Option{WithPrefix(":")}},
		{name: "zero TTL", options: []Option{WithTTL(0)}},
		{name: "negative database", options: []Option{WithDatabase(-1)}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := New(test.options...); err == nil {
				t.Fatal("New() error = nil, want configuration error")
			}
		})
	}
}

func TestPHPCompatibilityConfiguration(t *testing.T) {
	ttl := 24 * time.Minute
	store, err := New(WithPHPCompatibility(ttl))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if store.config.database != 1 {
		t.Fatalf("database = %d, want 1", store.config.database)
	}
	if store.config.prefix != PHPSessionPrefix {
		t.Fatalf("prefix = %q, want %q", store.config.prefix, PHPSessionPrefix)
	}
	if store.config.ttl != ttl {
		t.Fatalf("TTL = %s, want %s", store.config.ttl, ttl)
	}
}

func TestStoreSetGetAndExists(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	if err := store.Set(ctx, "bytes", []byte("value")); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	exists, err := store.Exists(ctx, "bytes")
	if err != nil || !exists {
		t.Fatalf("Exists() = (%v, %v), want (true, nil)", exists, err)
	}
	value, err := store.Get(ctx, "bytes")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(value) != "value" {
		t.Fatalf("Get() = %q, want %q", value, "value")
	}

	if err := store.SetString(ctx, "string", "text"); err != nil {
		t.Fatalf("SetString() error = %v", err)
	}
	text, err := store.GetString(ctx, "string")
	if err != nil {
		t.Fatalf("GetString() error = %v", err)
	}
	if text != "text" {
		t.Fatalf("GetString() = %q, want %q", text, "text")
	}
}

func TestStoreUsesPHPKeyAndPreservesJSONPayload(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), DB: 1})
	t.Cleanup(func() { _ = client.Close() })

	store, err := New(
		WithPHPCompatibility(30*time.Second),
		WithClient(client),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := store.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Stop(context.Background()) })

	ctx := context.Background()
	id := "php-session-id"
	payload := []byte(`{"UserRole":"Member","created":1}`)
	if err := store.Set(ctx, id, payload); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	key := "GLOBAL_SESSION:" + id
	got, err := server.DB(1).Get(key)
	if err != nil {
		t.Fatalf("miniredis Get() error = %v", err)
	}
	if got != string(payload) {
		t.Fatalf("Redis payload = %q, want %q", got, payload)
	}
	if ttl := server.DB(1).TTL(key); ttl <= 0 {
		t.Fatalf("Redis TTL = %s, want positive TTL", ttl)
	}

	value, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(value) != string(payload) {
		t.Fatalf("Get() = %q, want %q", value, payload)
	}
}

func TestStoreReadMatchesFileStoreHelper(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	id := "read-session"
	payload := []byte(`{"role":"member"}`)

	if err := store.Set(ctx, id, payload); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	value, err := store.Read(id, nil)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if string(value) != string(payload) {
		t.Fatalf("Read() = %q, want %q", value, payload)
	}

	if _, err := store.Read("missing", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Read() missing error = %v, want ErrNotFound", err)
	}
}

func TestStoreUsesNativeExpiration(t *testing.T) {
	store, server := newTestStore(t, WithTTL(10*time.Second))
	ctx := context.Background()

	if store.RequiresPurge() {
		t.Fatal("RequiresPurge() = true, want false")
	}
	if err := store.Set(ctx, "expiring", []byte(`{}`)); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if err := store.Purge(ctx, time.Nanosecond); err != nil {
		t.Fatalf("Purge() error = %v", err)
	}
	server.FastForward(11 * time.Second)

	exists, err := store.Exists(ctx, "expiring")
	if err != nil || exists {
		t.Fatalf("Exists() = (%v, %v), want (false, nil)", exists, err)
	}
}

func TestStoreTouchRefreshesTTLWithoutChangingPayload(t *testing.T) {
	store, server := newTestStore(t, WithTTL(10*time.Second))
	ctx := context.Background()
	id := "touch-session"
	payload := []byte(`{"role":"member"}`)

	if err := store.Set(ctx, id, payload); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	server.FastForward(8 * time.Second)
	if err := store.Touch(ctx, id); err != nil {
		t.Fatalf("Touch() error = %v", err)
	}
	server.FastForward(5 * time.Second)

	value, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(value) != string(payload) {
		t.Fatalf("Get() = %q, want unchanged payload %q", value, payload)
	}
}

func TestStoreMissingSession(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	if exists, err := store.Exists(ctx, "missing"); err != nil || exists {
		t.Fatalf("Exists() = (%v, %v), want (false, nil)", exists, err)
	}
	_, err := store.Get(ctx, "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get() error = %v, want ErrNotFound", err)
	}
	if err := store.Touch(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Touch() error = %v, want ErrNotFound", err)
	}
}

func TestStoreDeleteIsIdempotent(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	id := "delete-session"

	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("Delete() missing error = %v", err)
	}
	if err := store.Set(ctx, id, []byte(`{}`)); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("second Delete() error = %v", err)
	}
	if exists, err := store.Exists(ctx, id); err != nil || exists {
		t.Fatalf("Exists() after Delete = (%v, %v), want (false, nil)",
			exists, err)
	}
}

func TestStoreRejectsUnsafeSessionIDs(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	ids := []string{
		"",
		"../escape",
		"..",
		".",
		"nested/id",
		"contains.dot",
		`contains\slash`,
		"contains:colon",
		"contains space",
		"contains,comma",
	}

	for _, id := range ids {
		t.Run(id, func(t *testing.T) {
			if err := store.Set(ctx, id, []byte("blocked")); !errors.Is(err, ErrInvalidSessionID) {
				t.Errorf("Set() error = %v, want ErrInvalidSessionID", err)
			}
			if err := store.SetString(ctx, id, "blocked"); !errors.Is(err, ErrInvalidSessionID) {
				t.Errorf("SetString() error = %v, want ErrInvalidSessionID", err)
			}
			if err := store.Delete(ctx, id); !errors.Is(err, ErrInvalidSessionID) {
				t.Errorf("Delete() error = %v, want ErrInvalidSessionID", err)
			}
			if err := store.Touch(ctx, id); !errors.Is(err, ErrInvalidSessionID) {
				t.Errorf("Touch() error = %v, want ErrInvalidSessionID", err)
			}
			if _, err := store.Get(ctx, id); !errors.Is(err, ErrInvalidSessionID) {
				t.Errorf("Get() error = %v, want ErrInvalidSessionID", err)
			}
			if _, err := store.GetString(ctx, id); !errors.Is(err, ErrInvalidSessionID) {
				t.Errorf("GetString() error = %v, want ErrInvalidSessionID", err)
			}
			if _, err := store.Exists(ctx, id); !errors.Is(err, ErrInvalidSessionID) {
				t.Errorf("Exists() error = %v, want ErrInvalidSessionID", err)
			}
		})
	}
}

func TestStorePHPCompatibilityAcceptsCommaInSessionID(t *testing.T) {
	store, _ := newTestStore(t, WithPHPCompatibility(time.Minute))
	if err := store.Set(context.Background(), "php,id", []byte(`{}`)); err != nil {
		t.Fatalf("Set() PHP session ID error = %v", err)
	}
}

func TestStoreLifecycle(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, err := New(WithClient(client))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx := context.Background()

	if err := store.Set(ctx, "before-start", []byte(`{}`)); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("Set() before Start error = %v, want ErrNotStarted", err)
	}
	if err := store.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := store.Start(ctx); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("second Start() error = %v, want ErrAlreadyStarted", err)
	}
	if err := store.Stop(ctx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := store.Stop(ctx); err != nil {
		t.Fatalf("second Stop() error = %v", err)
	}
	if err := store.Start(ctx); err != nil {
		t.Fatalf("restart error = %v", err)
	}
	if err := store.Stop(ctx); err != nil {
		t.Fatalf("final Stop() error = %v", err)
	}
}

func TestStoreHandlesConcurrentAccess(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	keys := []string{"a", "b", "c", "d", "e"}
	errorsCh := make(chan error, len(keys))

	var wait sync.WaitGroup
	for _, key := range keys {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 100 {
				if err := store.Set(ctx, key, []byte("value")); err != nil {
					errorsCh <- err
					return
				}
				if err := store.Touch(ctx, key); err != nil {
					errorsCh <- err
					return
				}
				if _, err := store.Get(ctx, key); err != nil {
					errorsCh <- err
					return
				}
				if _, err := store.Exists(ctx, key); err != nil {
					errorsCh <- err
					return
				}
			}
		}()
	}
	wait.Wait()
	close(errorsCh)

	for err := range errorsCh {
		t.Errorf("concurrent operation error = %v", err)
	}
	for _, key := range keys {
		exists, err := store.Exists(ctx, key)
		if err != nil || !exists {
			t.Errorf("Exists(%q) = (%v, %v), want (true, nil)", key, exists, err)
		}
	}
}

func TestStoreEngineIntegration(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), DB: 1})
	t.Cleanup(func() { _ = client.Close() })
	const ttl = 30 * time.Minute

	store, err := New(
		WithPHPCompatibility(ttl),
		WithClient(client),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	engine := session.NewStoreEngine(
		store,
		session.WithProperties(&session.EngineProperties{
			AgeLimit: ttl,
			Name:     "PHPSESSID",
		}),
	)
	ctx := context.Background()
	if err := engine.Start(ctx); err != nil {
		t.Fatalf("engine.Start() error = %v", err)
	}
	t.Cleanup(func() { _ = engine.Stop(ctx) })

	id := "shared-session"
	if err := store.Set(ctx, id,
		[]byte(`{"UserRole":"Member","created":1,"unknown":"preserved"}`)); err != nil {
		t.Fatalf("Store.Set() error = %v", err)
	}
	loaded, err := engine.GetSession(ctx, id)
	if err != nil {
		t.Fatalf("GetSession() error = %v", err)
	}
	if loaded.Data["UserRole"] != "Member" {
		t.Fatalf("UserRole = %v, want Member", loaded.Data["UserRole"])
	}
	if loaded.Data["unknown"] != "preserved" {
		t.Fatalf("unknown = %v, want preserved", loaded.Data["unknown"])
	}
	if err := loaded.Set("updated_by", "go"); err != nil {
		t.Fatalf("Session.Set() error = %v", err)
	}
	if err := engine.SaveSession(ctx, id, loaded); err != nil {
		t.Fatalf("SaveSession() error = %v", err)
	}

	saved, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("Store.Get() error = %v", err)
	}
	var data map[string]any
	if err := engine.Properties().Encoder.Decode(saved, &data); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if data["updated_by"] != "go" || data["unknown"] != "preserved" {
		t.Fatalf("saved data = %#v, want update and preserved fields", data)
	}

	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("Store.Delete() error = %v", err)
	}
	if exists, err := store.Exists(ctx, id); err != nil || exists {
		t.Fatalf("Exists() after delete = (%v, %v), want (false, nil)", exists, err)
	}
}
