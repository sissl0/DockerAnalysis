package database

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisClient wraps the Redis client and provides methods to interact with Redis sets.
type RedisClient struct {
	client *redis.Client
}

// NewRedisClient initializes a new Redis client.
func NewRedisClient(addr string, password string, db int) *RedisClient {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	return &RedisClient{client: rdb}
}

// AddToSet adds a value to a Redis set.
func (r *RedisClient) AddToSet(ctx context.Context, key string, value string) (int64, error) {
	added, err := r.client.SAdd(ctx, key, value).Result()
	if err != nil {
		log.Printf("Failed to add value to set: %v", err)
		return -1, err
	}
	return added, nil
}

// RemoveFromSet removes a value from a Redis set.
func (r *RedisClient) RemoveFromSet(ctx context.Context, key string, value string) error {
	err := r.client.SRem(ctx, key, value).Err()
	if err != nil {
		log.Printf("Failed to remove value from set: %v", err)
		return err
	}
	return nil
}

// IsMember checks if a value is a member of a Redis set.
func (r *RedisClient) IsMember(ctx context.Context, key string, value string) (bool, error) {
	isMember, err := r.client.SIsMember(ctx, key, value).Result()
	if err != nil {
		log.Printf("Failed to check membership: %v", err)
		return false, err
	}
	return isMember, nil
}

// GetSetMembers retrieves all members of a Redis set.
func (r *RedisClient) GetSetMembers(ctx context.Context, key string) ([]string, error) {
	members, err := r.client.SMembers(ctx, key).Result()
	if err != nil {
		log.Printf("Failed to get set members: %v", err)
		return nil, err
	}
	return members, nil
}

func (r *RedisClient) WaitForRedis() error {
	ctx := context.Background()
	for {
		_, err := r.client.Ping(ctx).Result()
		if err == nil {
			fmt.Println("✅ Redis ist bereit")
			return nil
		}
		if err.Error() != "" && ( // explizit auf "LOADING" prüfen
		err.Error() == "LOADING Redis is loading the dataset in memory") {
			fmt.Println("⏳ Redis lädt noch, warte 500ms...")
			time.Sleep(500 * time.Millisecond)
			continue
		}
		return err
	}
}
