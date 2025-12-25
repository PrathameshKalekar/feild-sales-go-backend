package sync

import (
	"context"

	asynqutil "github.com/PrathameshKalekar/field-sales-go-backend/internal/asynq"
	"github.com/PrathameshKalekar/field-sales-go-backend/internal/config"
	"github.com/PrathameshKalekar/field-sales-go-backend/internal/tasks"
	"github.com/hibiken/asynq"
)

// HandleOrchestrateFullSyncTask handles the orchestration task
// This is called by the scheduler/worker, and it then orchestrates all sync tasks
func HandleOrchestrateFullSyncTask(ctx context.Context, t *asynq.Task) error {

	client := asynq.NewClient(asynqutil.ConnectToAsyncq(config.ConfigGlobal))
	defer client.Close()
	tasks.RunFullSyncOrchestration(client)

	return nil
}
