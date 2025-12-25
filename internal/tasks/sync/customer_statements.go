package sync

import (
	"context"
	"log"

	"github.com/hibiken/asynq"
)

func HandleSyncCustomerStatementsTask(ctx context.Context, t *asynq.Task) error {
	log.Println("Executing sync:customer_statements")
	return nil
}
