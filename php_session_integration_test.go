package redisstore

import (
	"context"
	"encoding/json"
	"net"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/candango/httpok/session"
)

const phpSessionTTL = 90 * time.Second

func TestPHPSessionHandlerRoundTrip(t *testing.T) {
	php := requirePHPRedis(t)
	server := miniredis.RunT(t)
	store, err := New(
		WithPHPCompatibility(phpSessionTTL),
		WithAddress(server.Addr()),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	engine := session.NewStoreEngine(
		store,
		session.WithProperties(&session.EngineProperties{
			AgeLimit: phpSessionTTL,
			Name:     "PHPSESSID",
		}),
	)
	ctx := context.Background()
	if err := engine.Start(ctx); err != nil {
		t.Fatalf("engine.Start() error = %v", err)
	}
	defer engine.Stop(ctx)

	const sessionID = "php-integration-session"
	if output := runPHP(t, php, server, sessionID, "seed"); output != "" {
		t.Fatalf("PHP seed output = %q, want empty output", output)
	}

	loaded, err := engine.GetSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("GetSession() after PHP seed error = %v", err)
	}
	if loaded.Data["UserRole"] != "Member" {
		t.Fatalf("UserRole = %v, want Member", loaded.Data["UserRole"])
	}
	if loaded.Data["from_php"] != "seeded" {
		t.Fatalf("from_php = %v, want seeded", loaded.Data["from_php"])
	}
	assertPHPKeyTTL(t, server, sessionID)

	if err := loaded.Set("from_go", "updated"); err != nil {
		t.Fatalf("Session.Set() error = %v", err)
	}
	if err := engine.SaveSession(ctx, sessionID, loaded); err != nil {
		t.Fatalf("SaveSession() error = %v", err)
	}

	phpData := decodePHPOutput(t, runPHP(t, php, server, sessionID, "read"))
	if phpData["from_go"] != "updated" {
		t.Fatalf("PHP did not observe Go update: %#v", phpData)
	}
	if phpData["UserRole"] != "Member" {
		t.Fatalf("PHP UserRole = %v, want Member", phpData["UserRole"])
	}

	if output := runPHP(t, php, server, sessionID, "set"); output != "" {
		t.Fatalf("PHP set output = %q, want empty output", output)
	}
	updated, err := store.Get(ctx, sessionID)
	if err != nil {
		t.Fatalf("Store.Get() after PHP update error = %v", err)
	}
	var updatedData map[string]any
	if err := json.Unmarshal(updated, &updatedData); err != nil {
		t.Fatalf("decode PHP JSON after update: %v", err)
	}
	if updatedData["from_php"] != "updated" {
		t.Fatalf("Go did not observe PHP update: %#v", updatedData)
	}
	assertPHPKeyTTL(t, server, sessionID)

	if output := runPHP(t, php, server, sessionID, "destroy"); output != "" {
		t.Fatalf("PHP destroy output = %q, want empty output", output)
	}
	exists, err := store.Exists(ctx, sessionID)
	if err != nil {
		t.Fatalf("Exists() after PHP destroy error = %v", err)
	}
	if exists {
		t.Fatal("session still exists after PHP destroy")
	}
}

func requirePHPRedis(t *testing.T) string {
	t.Helper()
	php, err := exec.LookPath("php")
	if err != nil {
		t.Fatalf("php is required for PHP integration tests: %v", err)
	}
	output, err := exec.Command(php, "-m").Output()
	if err != nil {
		t.Fatalf("php -m error: %v", err)
	}
	if !strings.Contains(strings.ToLower(string(output)), "redis") {
		t.Fatal("PHP Redis extension is not installed")
	}
	return php
}

func runPHP(
	t *testing.T,
	php string,
	server *miniredis.Miniredis,
	sessionID string,
	operation string,
) string {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller() failed")
	}
	script := filepath.Join(filepath.Dir(sourceFile), "testdata", "php_session.php")
	host, port, err := net.SplitHostPort(server.Addr())
	if err != nil {
		t.Fatalf("split miniredis address: %v", err)
	}
	command := exec.Command(
		php,
		script,
		host,
		port,
		sessionID,
		strconv.Itoa(int(phpSessionTTL/time.Second)),
		operation,
	)
	output, err := command.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			t.Fatalf("PHP helper failed: %v\n%s", err, exitErr.Stderr)
		}
		t.Fatalf("PHP helper failed: %v", err)
	}
	return strings.TrimSpace(string(output))
}

func decodePHPOutput(t *testing.T, output string) map[string]any {
	t.Helper()
	var data map[string]any
	if err := json.Unmarshal([]byte(output), &data); err != nil {
		t.Fatalf("decode PHP output: %v; output=%q", err, output)
	}
	return data
}

func assertPHPKeyTTL(t *testing.T, server *miniredis.Miniredis, sessionID string) {
	t.Helper()
	key := PHPSessionPrefix + ":" + sessionID
	if ttl := server.DB(1).TTL(key); ttl <= 0 {
		t.Fatalf("Redis TTL for %q = %s, want positive TTL", key, ttl)
	}
}
