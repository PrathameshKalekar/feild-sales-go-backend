package sync

import (
	"context"
	"log"

	"github.com/hibiken/asynq"
)

func HandleSyncOrdersTask(ctx context.Context, t *asynq.Task) error {
	log.Println("Executing sync:orders")
	return nil
}
