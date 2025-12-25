package sync

import (
	"context"
	"log"

	"github.com/hibiken/asynq"
)

func HandleSyncProductsTask(ctx context.Context, task *asynq.Task) error {
	log.Println("Executing sync:products")
	return nil
}
