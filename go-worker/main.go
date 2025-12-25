package main

import (
	asynqutil "github.com/PrathameshKalekar/field-sales-go-backend/internal/asynq"
	"github.com/PrathameshKalekar/field-sales-go-backend/internal/config"
	redisutil "github.com/PrathameshKalekar/field-sales-go-backend/internal/redis"
	"github.com/PrathameshKalekar/field-sales-go-backend/internal/tasks"
	syncutil "github.com/PrathameshKalekar/field-sales-go-backend/internal/tasks/sync"
	"github.com/hibiken/asynq"
)

func main() {
	config.Load()
	redisOpt := asynqutil.ConnectToAsyncq(config.ConfigGlobal)
	redisutil.ConnectToRedis(config.ConfigGlobal)
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

	// Register all task handlers (needed for workers to process individual tasks)
	mux.HandleFunc(tasks.SyncProducts, syncutil.HandleSyncProductsTask)
	mux.HandleFunc(tasks.SyncCustomers, syncutil.HandleSyncCustomersTask)
	mux.HandleFunc(tasks.SyncPricelists, syncutil.HandleSyncPricelistsTask)
	mux.HandleFunc(tasks.SyncCustomerStatements, syncutil.HandleSyncCustomerStatementsTask)
	mux.HandleFunc(tasks.SyncOrders, syncutil.HandleSyncOrdersTask)
	mux.HandleFunc(tasks.SyncInvoicesAndLines, syncutil.HandleSyncInvoicesAndLinesTask)

	// Register orchestration handler - this will orchestrate all sync tasks
	mux.HandleFunc(tasks.OrchestrateFullSync, syncutil.HandleOrchestrateFullSyncTask)

	scheduler := asynq.NewScheduler(redisOpt, nil)
	// Schedule orchestration task instead of individual tasks
	scheduler.Register("* * * * *", tasks.OrchestrateFullSyncTask())

	go func() {
		scheduler.Run()
	}()

	// Enqueue orchestration task immediately on startup (optional)
	asyncClient := asynq.NewClient(redisOpt)
	asyncClient.Enqueue(tasks.OrchestrateFullSyncTask())
	defer asyncClient.Close()

	asyncServer.Run(mux)
}
