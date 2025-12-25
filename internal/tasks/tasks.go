package tasks

import "github.com/hibiken/asynq"

const (
	SyncProducts = "sync:products"
)

func SyncProductsTask() *asynq.Task {
	return asynq.NewTask(SyncProducts, nil, asynq.MaxRetry(3))
}
