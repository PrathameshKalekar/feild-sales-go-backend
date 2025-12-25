package main

import (
	asynqutil "github.com/PrathameshKalekar/field-sales-go-backend/internal/asynq"
	"github.com/PrathameshKalekar/field-sales-go-backend/internal/config"
	"github.com/PrathameshKalekar/field-sales-go-backend/internal/tasks"
	syncutil "github.com/PrathameshKalekar/field-sales-go-backend/internal/tasks/sync"
	"github.com/hibiken/asynq"
)

func main() {

	cfg := config.Load()
	redisOpt := asynqutil.ConnectToAsyncq(cfg)

	asyncServer := asynq.NewServer(
		redisOpt,
		asynq.Config{
			Concurrency: 10,
			Queues: map[string]int{
				"default": 10,
			},
		},
	)

	mux := asynq.NewServeMux()
	mux.HandleFunc(tasks.SyncProducts, syncutil.HandleSyncProductsTask)

	scheduler := asynq.NewScheduler(redisOpt, nil)
	scheduler.Register("* * * * *", tasks.SyncProductsTask())

	go func() {
		scheduler.Run()
	}()

	asyncClient := asynq.NewClient(redisOpt)
	asyncClient.Enqueue(tasks.SyncProductsTask())

	asyncServer.Run(mux)
}
