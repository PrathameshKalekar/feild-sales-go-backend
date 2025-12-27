package tasks

import (
	"context"
	"log"
	"time"

	redisutil "github.com/PrathameshKalekar/field-sales-go-backend/internal/redis"
	"github.com/hibiken/asynq"
)

const (
	coreTasksCount  = 4
	orderTasksCount = 2
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

	// Initialize core tasks completion counter
	coreTasksKey := "core_tasks_remaining"
	if err := redisutil.RedisClient.Set(ctx, coreTasksKey, coreTasksCount, 0).Err(); err != nil {
		log.Printf("❌ Failed to set core tasks counter: %v", err)
		redisutil.RedisClient.Del(ctx, lockKey)
		return err
	}

	// Enqueue core tasks
	coreTasks := []*asynq.Task{
		SyncProductsTask(),
		SyncCustomersTask(),
		SyncPricelistsTask(),
		SyncCustomerStatementsTask(),
	}

	for _, task := range coreTasks {
		if _, err := client.Enqueue(task); err != nil {
			log.Printf("❌ Failed to enqueue core task: %v", err)
			redisutil.RedisClient.Del(ctx, lockKey)
			redisutil.RedisClient.Del(ctx, coreTasksKey)
			return err
		}
	}

	// Poll for core tasks completion (with timeout)
	timeout := 15 * time.Minute
	checkInterval := 2 * time.Second
	startTime := time.Now()

	for {
		if time.Since(startTime) > timeout {
			log.Printf("❌ Timeout waiting for core tasks to complete")
			redisutil.RedisClient.Del(ctx, lockKey)
			redisutil.RedisClient.Del(ctx, coreTasksKey)
			return nil
		}

		remaining, err := redisutil.RedisClient.Get(ctx, coreTasksKey).Int()
		if err != nil {
			// Key doesn't exist or error - assume tasks completed
			break
		}

		if remaining == 0 {
			log.Println("✅ All core tasks completed!")
			redisutil.RedisClient.Del(ctx, coreTasksKey)
			break
		}

		time.Sleep(checkInterval)
	}

	// Initialize order tasks completion counter
	orderTasksKey := "order_tasks_remaining"
	if err := redisutil.RedisClient.Set(ctx, orderTasksKey, orderTasksCount, 0).Err(); err != nil {
		log.Printf("❌ Failed to set order tasks counter: %v", err)
		redisutil.RedisClient.Del(ctx, lockKey)
		return err
	}

	// Enqueue order tasks group
	log.Println("📦 Group 2: Order Tasks - Enqueuing tasks in parallel...")
	log.Println("   - sync:orders")
	log.Println("   - sync:invoices_and_lines")

	orderTasks := []*asynq.Task{
		SyncOrdersTask(),
		SyncInvoicesAndLinesTask(),
	}

	for _, task := range orderTasks {
		if _, err := client.Enqueue(task); err != nil {
			log.Printf("❌ Failed to enqueue order task: %v", err)
			redisutil.RedisClient.Del(ctx, lockKey)
			redisutil.RedisClient.Del(ctx, orderTasksKey)
			return err
		}
	}

	log.Printf("✅ Order tasks group enqueued - %d tasks", len(orderTasks))

	// Poll for order tasks completion (with timeout)
	orderTimeout := 20 * time.Minute
	orderStartTime := time.Now()

	for {
		if time.Since(orderStartTime) > orderTimeout {
			log.Printf("❌ Timeout waiting for order tasks to complete")
			redisutil.RedisClient.Del(ctx, lockKey)
			redisutil.RedisClient.Del(ctx, orderTasksKey)
			return nil
		}

		remaining, err := redisutil.RedisClient.Get(ctx, orderTasksKey).Int()
		if err != nil {
			// Key doesn't exist or error - assume tasks completed
			break
		}

		if remaining == 0 {
			log.Println("✅ All order tasks completed!")
			redisutil.RedisClient.Del(ctx, orderTasksKey)
			break
		}

		time.Sleep(checkInterval)
	}

	// All tasks completed - release lock and log final message
	redisutil.RedisClient.Del(ctx, lockKey)
	log.Println("✅ Full sync completed!")

	return nil
}
