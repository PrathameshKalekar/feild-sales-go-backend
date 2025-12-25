package sync

import (
	"context"
	"log"

	"github.com/hibiken/asynq"
)

func HandleSyncInvoicesAndLinesTask(ctx context.Context, t *asynq.Task) error {
	log.Println("Executing sync:invoices_and_lines")
	return nil
}
