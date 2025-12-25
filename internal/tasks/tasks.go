package tasks

import "github.com/hibiken/asynq"

const (
	SyncProducts           = "sync:products"
	SyncCustomers          = "sync:customers"
	SyncPricelists         = "sync:pricelists"
	SyncCustomerStatements = "sync:customer_statements"
	SyncOrders             = "sync:orders"
	SyncInvoicesAndLines   = "sync:invoices_and_lines"
	ReleaseSyncLock        = "sync:release_lock"
	OrchestrateFullSync    = "sync:orchestrate_full"
)

func SyncProductsTask() *asynq.Task {
	return asynq.NewTask(SyncProducts, nil, asynq.MaxRetry(3))
}

func SyncCustomersTask() *asynq.Task {
	return asynq.NewTask(SyncCustomers, nil, asynq.MaxRetry(3))
}

func SyncPricelistsTask() *asynq.Task {
	return asynq.NewTask(SyncPricelists, nil, asynq.MaxRetry(3))
}

func SyncCustomerStatementsTask() *asynq.Task {
	return asynq.NewTask(SyncCustomerStatements, nil, asynq.MaxRetry(3))
}

func SyncOrdersTask() *asynq.Task {
	return asynq.NewTask(SyncOrders, nil, asynq.MaxRetry(3))
}

func SyncInvoicesAndLinesTask() *asynq.Task {
	return asynq.NewTask(SyncInvoicesAndLines, nil, asynq.MaxRetry(3))
}

func OrchestrateFullSyncTask() *asynq.Task {
	return asynq.NewTask(OrchestrateFullSync, nil, asynq.MaxRetry(3))
}
