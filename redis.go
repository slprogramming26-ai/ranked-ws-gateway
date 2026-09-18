package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// newRedisClient baut den EINEN Redis-Client, den sich alle Verbindungen teilen.
// Er hat intern einen Verbindungspool und ist von beliebig vielen Goroutines
// gleichzeitig benutzbar – niemals einen Client pro Socket anlegen.
func newRedisClient(redisURL string) (*redis.Client, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("REDIS_URL unbrauchbar: %w", err)
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("Redis nicht erreichbar: %w", err)
	}

	return client, nil
}
