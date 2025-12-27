package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/PrathameshKalekar/field-sales-go-backend/internal/odoo"
	redisutil "github.com/PrathameshKalekar/field-sales-go-backend/internal/redis"
	typesenseutil "github.com/PrathameshKalekar/field-sales-go-backend/internal/typesense"
	"github.com/hibiken/asynq"
	"github.com/typesense/typesense-go/v4/typesense/api"
)

func HandleSyncOrdersTask(ctx context.Context, t *asynq.Task) error {
	log.Println("🔄 Starting orders sync...")

	// Ensure schema exists
	if err := ensureOrdersSchema(ctx); err != nil {
		log.Printf("❌ Failed to ensure schema: %v", err)
		return err
	}

	fields := []string{
		"id", "name", "partner_id", "amount_total", "date_order",
		"expected_date", "amount_to_invoice",
		"delivery_status", "amount_unpaid", "invoice_status",
	}

	limit := 1000
	offset := 0
	page := 1
	totalIndexed := 0

	// Calculate 6 months ago (180 days)
	sixMonthsAgo := time.Now().UTC().AddDate(0, 0, -180)
	domain := []any{
		[]any{"date_order", ">", sixMonthsAgo.Format("2006-01-02 15:04:05")},
	}

	pipe := redisutil.RedisClient.Pipeline()

	for {
		payload := map[string]any{
			"jsonrpc": "2.0",
			"method":  "call",
			"params": map[string]any{
				"model":  "sale.order",
				"method": "search_read",
				"args":   []any{domain},
				"kwargs": map[string]any{
					"fields": fields,
					"offset": offset,
					"limit":  limit,
				},
			},
			"id": page,
		}

		resp, err := odoo.OdooManager.NewRequest("POST", "web/dataset/call_kw", payload)
		if err != nil {
			log.Printf("❌ Failed to fetch orders batch: %v", err)
			return err
		}

		var rpcResp struct {
			Result []map[string]any `json:"result"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
			resp.Body.Close()
			log.Printf("❌ Failed to decode response: %v", err)
			return err
		}
		resp.Body.Close()

		orders := rpcResp.Result
		if len(orders) == 0 {
			break
		}

		// Clean orders
		cleaned := make([]map[string]any, 0, len(orders))
		for _, order := range orders {
			cleaned = append(cleaned, cleanOrder(order))
		}

		// Redis - add to pipeline
		cleanedJSON, err := json.Marshal(cleaned)
		if err != nil {
			log.Printf("⚠️  Failed to marshal orders for page %d: %v", page, err)
		} else {
			pipe.Set(ctx, fmt.Sprintf("orders_page:%d", page), cleanedJSON, 0)
		}

		// Typesense - import page
		documents := make([]any, len(cleaned))
		for i, c := range cleaned {
			documents[i] = c
		}

		action := api.IndexAction("upsert")
		_, err = typesenseutil.TypesenseClient.Collection("orders").Documents().Import(ctx, documents, &api.ImportDocumentsParams{
			Action: &action,
		})
		if err != nil {
			log.Printf("❌ Typesense import failed on page %d: %v", page, err)
		} else {
			totalIndexed += len(cleaned)
		}

		log.Printf("📦 Page %d synced (%d orders)", page, len(cleaned))

		offset += limit
		page++
	}

	// Execute Redis pipeline
	_, err := pipe.Exec(ctx)
	if err != nil {
		log.Printf("❌ Redis couldn't save cached orders master: %v", err)
	}

	log.Printf("✅ Orders sync completed. Total indexed: %d", totalIndexed)
	return nil
}

// cleanOrder cleans and transforms an order from Odoo format to our format
func cleanOrder(order map[string]any) map[string]any {
	get := func(key string) any {
		return order[key]
	}

	// Parse Odoo datetime safely and convert to timestamp
	isoDate := getString(get("date_order"))
	var timestamp int64 = 0
	if isoDate != "" {
		// Replace space with T for ISO format
		isoDateFixed := strings.Replace(isoDate, " ", "T", 1)
		parsedTime, err := time.Parse("2006-01-02T15:04:05", isoDateFixed)
		if err != nil {
			// Try with microseconds
			parsedTime, err = time.Parse("2006-01-02T15:04:05.000000", isoDateFixed)
			if err != nil {
				// Try without time
				parsedTime, err = time.Parse("2006-01-02", isoDate)
				if err != nil {
					timestamp = 0
				} else {
					timestamp = parsedTime.Unix()
				}
			} else {
				timestamp = parsedTime.Unix()
			}
		} else {
			timestamp = parsedTime.Unix()
		}
	}

	// Handle partner_id correctly (both ID and Name)
	partnerID := 0
	partnerName := "NA"
	if partner, ok := get("partner_id").([]any); ok && len(partner) == 2 {
		partnerID = getInt(partner[0])
		partnerName = getString(partner[1])
	}

	return map[string]any{
		"id":                fmt.Sprintf("%v", get("id")),
		"name":              getStringOrNA(get("name")),
		"partner_id":        partnerID,
		"partner_name":      partnerName,
		"amount_total":      getFloat(get("amount_total")),
		"date_order":        isoDate,
		"date_order_ts":     timestamp,
		"expected_date":     getStringOrNA(get("expected_date")),
		"amount_to_invoice": getFloat(get("amount_to_invoice")),
		"delivery_status":   getStringOrNA(get("delivery_status")),
		"amount_unpaid":     getFloat(get("amount_unpaid")),
		"invoice_status":    getStringOrNA(get("invoice_status")),
	}
}

// ensureOrdersSchema ensures the Typesense orders collection schema exists
func ensureOrdersSchema(ctx context.Context) error {
	// Try to retrieve the collection
	_, err := typesenseutil.TypesenseClient.Collection("orders").Retrieve(ctx)
	if err == nil {
		return nil
	}

	// Collection doesn't exist, create it
	log.Println("📦 Creating new Typesense schema for orders")

	sortTrue := true
	defaultSortingField := "date_order_ts"
	schema := &api.CollectionSchema{
		Name: "orders",
		Fields: []api.Field{
			{Name: "id", Type: "string"},
			{Name: "name", Type: "string"},
			{Name: "partner_id", Type: "int32", Facet: &sortTrue},
			{Name: "partner_name", Type: "string"},
			{Name: "amount_total", Type: "float", Sort: &sortTrue},
			{Name: "date_order", Type: "string", Sort: &sortTrue},
			{Name: "date_order_ts", Type: "int64", Facet: &sortTrue, Sort: &sortTrue},
			{Name: "expected_date", Type: "string"},
			{Name: "amount_to_invoice", Type: "float"},
			{Name: "delivery_status", Type: "string", Facet: &sortTrue},
			{Name: "amount_unpaid", Type: "float"},
			{Name: "invoice_status", Type: "string", Facet: &sortTrue},
		},
		DefaultSortingField: &defaultSortingField,
	}

	_, err = typesenseutil.TypesenseClient.Collections().Create(ctx, schema)
	if err != nil {
		return fmt.Errorf("failed to create orders collection: %w", err)
	}

	return nil
}

// getStringOrNA returns "NA" if value is nil or empty, otherwise returns string representation
func getStringOrNA(v any) string {
	if v == nil {
		return "NA"
	}
	str := fmt.Sprintf("%v", v)
	if str == "" {
		return "NA"
	}
	return str
}
