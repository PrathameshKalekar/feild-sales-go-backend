package tasks

import (
	"context"
	"log"
	"time"

	redisutil "github.com/PrathameshKalekar/field-sales-go-backend/internal/redis"
	"github.com/hibiken/asynq"
)

func RunFullSyncOrchestration(client *asynq.Client) error {
	log.Println("🔄 Starting full sync orchestration...")

	ctx := context.Background()
	lockKey := "sync_running"

	locked, err := redisutil.RedisClient.SetNX(ctx, lockKey, "1", 10*time.Minute).Result()
	if err != nil {
		log.Printf("❌ Failed to acquire sync lock: %v", err)
		return err
	}

	if !locked {
		log.Println("⚠️  Lock exists, skipping the sync")
		return nil
	}

	coreTasks := []*asynq.Task{
		SyncProductsTask(),
		SyncCustomersTask(),
		SyncPricelistsTask(),
		SyncCustomerStatementsTask(),
	}

	for _, task := range coreTasks {
		client.Enqueue(task)
	}

	orderTasks := []*asynq.Task{
		SyncOrdersTask(),
		SyncInvoicesAndLinesTask(),
	}

	for _, task := range orderTasks {
		client.Enqueue(task)
	}

	redisutil.RedisClient.Del(ctx, lockKey)

	return nil
}
