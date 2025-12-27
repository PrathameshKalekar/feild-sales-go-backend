package sync

import (
	"context"
	"log"

	"github.com/hibiken/asynq"
)

func HandleSyncCustomerStatementsTask(ctx context.Context, t *asynq.Task) error {
	defer MarkCoreTaskCompletion(ctx)
	log.Println("Executing sync:customer_statements")
	return nil
}
