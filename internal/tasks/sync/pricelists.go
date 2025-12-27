package sync

import (
	"context"
	"log"

	"github.com/hibiken/asynq"
)

func HandleSyncPricelistsTask(ctx context.Context, t *asynq.Task) error {
	defer MarkCoreTaskCompletion(ctx)
	log.Println("Executing sync:pricelists")
	return nil
}
