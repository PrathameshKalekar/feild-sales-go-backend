package sync

import (
	"context"
	"log"

	"github.com/hibiken/asynq"
)

func HandleSyncCustomersTask(ctx context.Context, t *asynq.Task) error {
	log.Println("Executing sync:customers")
	return nil
}
