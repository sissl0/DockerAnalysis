/*
Georg Heindl
Hilfsfunktionen zum Interagieren mit Redis.
Wrapper für go-redis.
*/
package database

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	client *redis.Client
}

/*
NewRedisClient erstellt einen neuen Redis-Client.
*/
func NewRedisClient(addr string, password string, db int) *RedisClient {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	return &RedisClient{client: rdb}
}

/*
AddToSet fügt einen Wert zu einem Redis-Set hinzu.
*/
func (r *RedisClient) AddToSet(ctx context.Context, key string, value string) (int64, error) {
	added, err := r.client.SAdd(ctx, key, value).Result()
	if err != nil {
		log.Printf("Failed to add value to set: %v", err)
		return -1, err
	}
	return added, nil
}

/*
RemoveFromSet entfernt einen Wert aus einem Redis-Set.
*/
func (r *RedisClient) RemoveFromSet(ctx context.Context, key string, value string) error {
	err := r.client.SRem(ctx, key, value).Err()
	if err != nil {
		log.Printf("Failed to remove value from set: %v", err)
		return err
	}
	return nil
}

/*
IsMember prüft, ob ein Wert Mitglied eines Redis-Sets ist.
*/
func (r *RedisClient) IsMember(ctx context.Context, key string, value string) (bool, error) {
	isMember, err := r.client.SIsMember(ctx, key, value).Result()
	if err != nil {
		log.Printf("Failed to check membership: %v", err)
		return false, err
	}
	return isMember, nil
}

/*
GetSetMembers gibt alle Mitglieder eines Redis-Sets zurück.
*/
func (r *RedisClient) GetSetMembers(ctx context.Context, key string) ([]string, error) {
	members, err := r.client.SMembers(ctx, key).Result()
	if err != nil {
		log.Printf("Failed to get set members: %v", err)
		return nil, err
	}
	return members, nil
}

/*
WaitForRedis wartet, bis Redis alle Sets in RAM geladen hat.
*/
func (r *RedisClient) WaitForRedis() error {
	ctx := context.Background()
	for {
		_, err := r.client.Ping(ctx).Result()
		if err == nil {
			fmt.Println("Redis Read")
			return nil
		}
		if err.Error() != "" && ( // explizit auf "LOADING" prüfen
		err.Error() == "LOADING Redis is loading the dataset in memory") {
			fmt.Println("Redis Loading, waiting...")
			time.Sleep(500 * time.Millisecond)
			continue
		}
		return err
	}
}
