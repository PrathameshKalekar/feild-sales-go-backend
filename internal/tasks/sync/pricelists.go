package sync

import (
	"context"
	"log"

	"github.com/hibiken/asynq"
)

func HandleSyncPricelistsTask(ctx context.Context, t *asynq.Task) error {
	log.Println("Executing sync:pricelists")
	return nil
}
