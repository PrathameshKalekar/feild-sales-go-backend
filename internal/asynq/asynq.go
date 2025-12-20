package asynqUtil

import (
	"log"
	"net/url"
	"strconv"

	"github.com/PrathameshKalekar/field-sales-go-backend/internal/config"
	"github.com/hibiken/asynq"
)

var AsyncqClient *asynq.RedisClientOpt

func ConnectToAsyncq(config *config.Config) {
	url, err := url.Parse(config.RedisUrl)
	if err != nil {
		log.Println("Error while parsing redis url")
	}
	db := 0
	if url.Path != "" {
		db, _ = strconv.Atoi(url.Path[1:])

	}

	AsyncqClient = &asynq.RedisClientOpt{
		Addr: url.Host,
		DB:   db,
	}
}
