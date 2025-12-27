package sync

import (
	"context"
	"log"

	redisutil "github.com/PrathameshKalekar/field-sales-go-backend/internal/redis"
)

// MarkCoreTaskCompletion decrements the core tasks counter when a core task completes
func MarkCoreTaskCompletion(ctx context.Context) {
	coreTasksKey := "core_tasks_remaining"
	remaining, err := redisutil.RedisClient.Decr(ctx, coreTasksKey).Result()
	if err != nil {
		log.Printf("⚠️  Failed to decrement core tasks counter: %v", err)
		return
	}
	log.Printf("📊 Core tasks remaining: %d", remaining)
}
