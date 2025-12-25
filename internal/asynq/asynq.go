package asynqutil

import (
	"log"
	"net/url"
	"strconv"

	"github.com/PrathameshKalekar/field-sales-go-backend/internal/config"
	"github.com/hibiken/asynq"
)

func ConnectToAsyncq(config *config.Config) *asynq.RedisClientOpt {
	url, err := url.Parse(config.RedisUrl)
	if err != nil {
		log.Println("Error while parsing redis url")
	}
	db := 0
	if url.Path != "" {
		db, _ = strconv.Atoi(url.Path[1:])

	}

	return &asynq.RedisClientOpt{
		Addr: url.Host,
		DB:   db,
	}
}
