package syncutil

import (
	"context"
	"log"

	"github.com/hibiken/asynq"
)

func HandleSyncProductsTask(ctx context.Context, task *asynq.Task) error {
	log.Println("Called for product sync")
	// TODO: Implement actual sync logic here
	return nil
}
