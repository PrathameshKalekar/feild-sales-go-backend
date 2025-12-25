package redisutil

import (
	"context"
	"log"
	"net/url"
	"strconv"

	"github.com/PrathameshKalekar/field-sales-go-backend/internal/config"
	"github.com/redis/go-redis/v9"
)

var RedisClient *redis.Client

func ConnectToRedis(config *config.Config) {
	// Parse Redis URL (format: redis://host:port/db)
	redisURL, err := url.Parse(config.RedisUrl)
	if err != nil {
		log.Fatalf("Error parsing Redis URL: %v", err)
	}

	addr := redisURL.Host
	db := 0
	if redisURL.Path != "" && len(redisURL.Path) > 1 {
		db, _ = strconv.Atoi(redisURL.Path[1:])
	}

	RedisClient = redis.NewClient(&redis.Options{
		Addr: addr,
		DB:   db,
	})
	_, err = RedisClient.Ping(context.Background()).Result()
	if err != nil {
		log.Fatalf("Error while connecting redis: %v", err)
	}
	log.Println("Redis Connected Successfully")
}
