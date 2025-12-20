package redisutil

import (
	"context"
	"log"

	"github.com/PrathameshKalekar/field-sales-go-backend/internal/config"
	"github.com/redis/go-redis/v9"
)

var RedisClient *redis.Client

func ConnectToRedis(config *config.Config) {
	RedisClient = redis.NewClient(&redis.Options{
		Addr: config.RedisUrl,
	})
	_, err := RedisClient.Ping(context.Background()).Result()
	if err != nil {
		log.Fatal("Error while connecting redis : ", err)
	}
	log.Println("Redis Connected Successfully")
}
