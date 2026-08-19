package redisstore_test

import (
	"context"
	"fmt"
	"time"

	"github.com/alicebob/miniredis/v2"
	redisstore "github.com/candango/httpok-redis"
	"github.com/candango/httpok/session"
	"github.com/redis/go-redis/v9"
)

func ExampleStore_phpSessionSharing() {
	server, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer server.Close()

	client := redis.NewClient(&redis.Options{Addr: server.Addr(), DB: 1})
	defer client.Close()

	const ttl = 24 * time.Minute
	store, err := redisstore.New(
		redisstore.WithPHPCompatibility(ttl),
		redisstore.WithClient(client),
	)
	if err != nil {
		panic(err)
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
		panic(err)
	}
	defer engine.Stop(ctx)

	if err := store.Set(ctx, "legacy-id", []byte(`{"UserRole":"Member"}`)); err != nil {
		panic(err)
	}
	shared, err := engine.GetSession(ctx, "legacy-id")
	if err != nil {
		panic(err)
	}
	fmt.Println(shared.Data["UserRole"])

	// Output: Member
}
