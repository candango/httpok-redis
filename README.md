# httpok-redis

Optional Redis integration for [`github.com/candango/httpok`](https://github.com/candango/httpok).

The core `httpok` module owns the session contracts and does not depend on
Redis. This module owns the Redis client and implements `httpok/session.Store`.

## Generic usage

```go
store, err := redisstore.New(
    redisstore.WithAddress("127.0.0.1:6379"),
    redisstore.WithDatabase(0),
    redisstore.WithPrefix("httpok:session"),
    redisstore.WithTTL(30*time.Minute),
)
if err != nil {
    return err
}

engine := session.NewStoreEngine(store)
if err := engine.Start(ctx); err != nil {
    return err
}
defer engine.Stop(ctx)
```

## PHP/Firenado compatibility

The legacy PHP handler used by Firenado stores the complete session payload as
JSON in Redis database 1 using keys shaped as `GLOBAL_SESSION:<PHPSESSID>`.
Configure the adapter with the compatibility option:

```go
store, err := redisstore.New(
    redisstore.WithPHPCompatibility(30 * time.Minute),
)
```

The PHP and Go applications must agree on the TTL. The cookie is the opaque
`PHPSESSID`; no signing secret is required for this shared-cookie mode.

`Set` uses Redis native expiration atomically, and `Touch` refreshes the TTL
without changing the JSON payload. Redis owns expiration, so `RequiresPurge`
returns false.

## Store contract and tests

The adapter implements the complete `session.Store` surface: lifecycle,
byte/string reads and writes, existence checks, idempotent deletion, native
expiration, touch, and purge capability reporting. It also exposes the
legacy `Read(id, any)` helper provided by `FileStore`. Session IDs are
validated before they become Redis keys; the default allowlist matches
`FileStore`, while the PHP compatibility preset also accepts comma-separated
PHP IDs.

Unit tests use `miniredis`, which is imported only by `*_test.go` files. It is
not part of the production package dependency graph. The executable Go example
in `example_test.go` verifies PHP-compatible session sharing through
`session.StoreEngine`.

```bash
go test ./...
go vet ./...
go test -race ./...
```

## Compatibility

The module is validated against the published `httpok` session Store
contract. Redis remains an optional dependency owned exclusively by this
module.
