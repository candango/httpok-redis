// Package redisstore provides a Redis-backed implementation of httpok's
// session.Store contract.
package redisstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/candango/httpok/session"
	"github.com/redis/go-redis/v9"
)

const (
	// DefaultAddress is the default Redis server address.
	DefaultAddress = "127.0.0.1:6379"
	// DefaultDatabase is the default Redis logical database.
	DefaultDatabase = 0
	// DefaultPrefix is the default namespace for httpok sessions.
	DefaultPrefix = session.DefaultPrefix
	// PHPSessionPrefix is the namespace used by the legacy PHP handler.
	PHPSessionPrefix = "GLOBAL_SESSION"
	// DefaultTTL is the default Redis expiration for session entries.
	DefaultTTL = 30 * time.Minute
)

var (
	// ErrAlreadyStarted indicates that Start was called on a running Store.
	ErrAlreadyStarted = errors.New("redis store already started")
	// ErrInvalidSessionID indicates that an ID cannot safely form a session key.
	ErrInvalidSessionID = errors.New("invalid session id")
	// ErrNotFound indicates that a session entry does not exist.
	ErrNotFound = errors.New("redis session not found")
	// ErrNotStarted indicates that an operation requires Start to be called.
	ErrNotStarted = errors.New("redis store is not started")
)

type config struct {
	address    string
	username   string
	password   string
	database   int
	prefix     string
	ttl        time.Duration
	client     *redis.Client
	allowComma bool
}

// Option configures a Redis Store.
type Option func(*config)

// WithAddress configures the Redis server address.
func WithAddress(address string) Option {
	return func(c *config) {
		c.address = address
	}
}

// WithCredentials configures Redis authentication credentials.
func WithCredentials(username, password string) Option {
	return func(c *config) {
		c.username = username
		c.password = password
	}
}

// WithDatabase configures the Redis logical database.
func WithDatabase(database int) Option {
	return func(c *config) {
		c.database = database
	}
}

// WithPrefix configures the Redis key namespace. A trailing colon is removed
// so keys are consistently rendered as <prefix>:<session-id>.
func WithPrefix(prefix string) Option {
	return func(c *config) {
		c.prefix = strings.TrimSuffix(prefix, ":")
	}
}

// WithTTL configures the expiration applied by Set and Touch.
func WithTTL(ttl time.Duration) Option {
	return func(c *config) {
		c.ttl = ttl
	}
}

// WithClient injects an existing Redis client. The client owns its connection
// settings, including the logical database, and Store does not close it during
// Stop.
func WithClient(client *redis.Client) Option {
	return func(c *config) {
		c.client = client
	}
}

// WithPHPCompatibility configures the key namespace used by the legacy PHP
// RedisSessionHandler. The PHP process and this Store must use the same TTL.
func WithPHPCompatibility(ttl time.Duration) Option {
	return func(c *config) {
		c.database = 1
		c.prefix = PHPSessionPrefix
		c.ttl = ttl
		c.allowComma = true
	}
}

// Store implements session.Store using Redis native expiration.
type Store struct {
	config config

	mu       sync.RWMutex
	client   *redis.Client
	ownsConn bool
	started  bool
}

var _ session.Store = (*Store)(nil)

// New creates a Redis-backed session Store. Redis connectivity is checked by
// Start, not by New.
func New(options ...Option) (*Store, error) {
	cfg := config{
		address:  DefaultAddress,
		database: DefaultDatabase,
		prefix:   DefaultPrefix,
		ttl:      DefaultTTL,
	}
	for _, option := range options {
		option(&cfg)
	}
	if cfg.client == nil && cfg.address == "" {
		return nil, errors.New("redis address is empty")
	}
	if cfg.prefix == "" {
		return nil, errors.New("redis session prefix is empty")
	}
	if cfg.ttl <= 0 {
		return nil, errors.New("redis session TTL must be positive")
	}
	if cfg.database < 0 {
		return nil, errors.New("redis database must not be negative")
	}
	return &Store{config: cfg, client: cfg.client}, nil
}

// Start creates or validates the Redis client and checks connectivity.
func (s *Store) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return ErrAlreadyStarted
	}

	client := s.config.client
	ownsConn := false
	if client == nil {
		client = redis.NewClient(&redis.Options{
			Addr:     s.config.address,
			Username: s.config.username,
			Password: s.config.password,
			DB:       s.config.database,
		})
		ownsConn = true
	}
	if err := client.Ping(ctx).Err(); err != nil {
		if ownsConn {
			_ = client.Close()
		}
		return fmt.Errorf("ping Redis: %w", err)
	}

	s.client = client
	s.ownsConn = ownsConn
	s.started = true
	return nil
}

// Stop closes a client created by Store. Injected clients remain owned by the
// caller.
func (s *Store) Stop(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return nil
	}
	var err error
	if s.ownsConn && s.client != nil {
		err = s.client.Close()
	}
	s.client = nil
	s.ownsConn = false
	s.started = false
	return err
}

// RequiresPurge reports that Redis expires entries natively.
func (s *Store) RequiresPurge() bool {
	return false
}

// Purge is a no-op because Redis owns expiration through key TTLs.
func (s *Store) Purge(context.Context, time.Duration) error {
	return nil
}

// Delete removes a session entry. Deleting a missing entry is successful.
func (s *Store) Delete(ctx context.Context, id string) error {
	key, err := s.sessionKey(id)
	if err != nil {
		return err
	}
	client, err := s.redisClient()
	if err != nil {
		return err
	}
	return client.Del(ctx, key).Err()
}

// Exists reports whether the session entry exists.
func (s *Store) Exists(ctx context.Context, id string) (bool, error) {
	key, err := s.sessionKey(id)
	if err != nil {
		return false, err
	}
	client, err := s.redisClient()
	if err != nil {
		return false, err
	}
	count, err := client.Exists(ctx, key).Result()
	return count > 0, err
}

// Get retrieves the raw session payload.
func (s *Store) Get(ctx context.Context, id string) ([]byte, error) {
	key, err := s.sessionKey(id)
	if err != nil {
		return nil, err
	}
	client, err := s.redisClient()
	if err != nil {
		return nil, err
	}
	value, err := client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	return value, err
}

// Read retrieves the raw session payload using the legacy FileStore helper
// shape. It does not refresh the TTL; callers that need a sliding expiration
// should use Touch or session.StoreEngine.GetSession.
func (s *Store) Read(id string, _ any) ([]byte, error) {
	return s.Get(context.Background(), id)
}

// GetString retrieves the session payload as a string.
func (s *Store) GetString(ctx context.Context, id string) (string, error) {
	value, err := s.Get(ctx, id)
	if err != nil {
		return "", err
	}
	return string(value), nil
}

// Set stores a raw session payload and applies the configured TTL atomically.
func (s *Store) Set(ctx context.Context, id string, value []byte) error {
	key, err := s.sessionKey(id)
	if err != nil {
		return err
	}
	client, err := s.redisClient()
	if err != nil {
		return err
	}
	return client.Set(ctx, key, value, s.config.ttl).Err()
}

// SetString stores a string session payload and applies the configured TTL.
func (s *Store) SetString(ctx context.Context, id, value string) error {
	return s.Set(ctx, id, []byte(value))
}

// Touch refreshes the TTL without changing the session payload.
func (s *Store) Touch(ctx context.Context, id string) error {
	key, err := s.sessionKey(id)
	if err != nil {
		return err
	}
	client, err := s.redisClient()
	if err != nil {
		return err
	}
	updated, err := client.Expire(ctx, key, s.config.ttl).Result()
	if err != nil {
		return err
	}
	if !updated {
		return ErrNotFound
	}
	return nil
}

func (s *Store) redisClient() (*redis.Client, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.started || s.client == nil {
		return nil, ErrNotStarted
	}
	return s.client, nil
}

func (s *Store) sessionKey(id string) (string, error) {
	if !validSessionID(id, s.config.allowComma) {
		return "", ErrInvalidSessionID
	}
	return s.config.prefix + ":" + id, nil
}

func validSessionID(id string, allowComma bool) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, char := range id {
		if (char < 'a' || char > 'z') &&
			(char < 'A' || char > 'Z') &&
			(char < '0' || char > '9') &&
			char != '-' && char != '_' && (char != ',' || !allowComma) {
			return false
		}
	}
	return true
}
