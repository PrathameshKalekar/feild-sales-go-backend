package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/PrathameshKalekar/field-sales-go-backend/internal/odoo"
	redisutil "github.com/PrathameshKalekar/field-sales-go-backend/internal/redis"
	typesenseutil "github.com/PrathameshKalekar/field-sales-go-backend/internal/typesense"
	"github.com/hibiken/asynq"
	"github.com/typesense/typesense-go/v4/typesense/api"
)

func HandleSyncCustomersTask(ctx context.Context, t *asynq.Task) error {
	defer MarkCoreTaskCompletion(ctx)
	log.Println("🔄 Starting customer sync...")

	fields := []string{
		"id", "x_studio_account_number", "display_name", "email",
		"property_payment_term_id", "phone", "city", "hold_delivery_till_payment",
		"credit_hold", "has_overdue_by_x_days", "total_overdue", "credit",
		"zip", "days_sales_outstanding", "user_id", "property_product_pricelist",
	}

	offset, limit, pageNum := 0, 1000, 1
	allCustomers := []map[string]any{}

	for {
		batch, err := fetchCustomers(ctx, offset, limit, fields)
		if err != nil {
			log.Printf("❌ Failed to fetch customers batch: %v", err)
			return err
		}

		if len(batch) == 0 {
			break
		}

		pageDocs := []map[string]any{}
		for _, c := range batch {
			customerDoc := cleanCustomer(c)

			pageDocs = append(pageDocs, customerDoc)
			allCustomers = append(allCustomers, customerDoc)

			// Save each customer in Redis as hash
			customerID := int(c["id"].(float64))
			customerKey := fmt.Sprintf("customers:%d", customerID)

			// Delete existing customer data
			redisutil.RedisClient.Del(ctx, customerKey)

			// Convert to map[string]string for HSET
			sanitizedDoc := make(map[string]string)
			for k, v := range customerDoc {
				switch val := v.(type) {
				case bool:
					sanitizedDoc[k] = strconv.FormatBool(val)
				case []string, []int, []float64:
					// Convert lists to JSON string
					jsonBytes, _ := json.Marshal(val)
					sanitizedDoc[k] = string(jsonBytes)
				default:
					sanitizedDoc[k] = fmt.Sprintf("%v", val)
				}
			}

			// Save as hash
			if err := redisutil.RedisClient.HSet(ctx, customerKey, sanitizedDoc).Err(); err != nil {
				log.Printf("⚠️  Failed to save customer %d to Redis: %v", customerID, err)
			}

			// Add to customers set
			redisutil.RedisClient.SAdd(ctx, "customers", customerID)
		}

		// Cache page separately
		pageJSON, err := json.Marshal(pageDocs)
		if err != nil {
			log.Printf("⚠️  Failed to marshal page: %v", err)
		} else {
			redisutil.RedisClient.Set(ctx, fmt.Sprintf("customers_page:%d", pageNum), pageJSON, 0)
		}

		log.Printf("✅ Customers - Batch %d done", pageNum)

		offset += limit
		pageNum++
	}

	// Ensure Typesense schema exists
	if err := ensureCustomersSchema(ctx); err != nil {
		log.Printf("❌ Failed to ensure schema: %v", err)
		return err
	}

	// Wipe old docs and bulk import
	if len(allCustomers) > 0 {
		// Delete old documents
		filterBy := "customer_id:>0"
		_, err := typesenseutil.TypesenseClient.Collection("customers").Documents().Delete(ctx, &api.DeleteDocumentsParams{
			FilterBy: &filterBy,
		})
		if err != nil {
			log.Printf("⚠️  Could not delete old documents (collection may not exist): %v", err)
		}

		// Convert to []any for Import
		documents := make([]any, len(allCustomers))
		for i, c := range allCustomers {
			documents[i] = c
		}

		// Bulk import
		action := api.IndexAction("upsert")
		importResult, err := typesenseutil.TypesenseClient.Collection("customers").Documents().Import(ctx, documents, &api.ImportDocumentsParams{
			Action: &action,
		})
		if err != nil {
			log.Printf("❌ Failed to import customers to Typesense: %v", err)
			return err
		}

		log.Printf("✅ Synced %d customers into Redis + Typesense. Imported %d documents", len(allCustomers), len(importResult))
	}

	log.Println("✅ Customer sync completed!")
	return nil
}

// fetchCustomers fetches customers from Odoo in batches
func fetchCustomers(ctx context.Context, offset, limit int, fields []string) ([]map[string]any, error) {
	payload := map[string]any{
		"jsonrpc": "2.0",
		"method":  "call",
		"params": map[string]any{
			"model":  "res.partner",
			"method": "search_read",
			"args": []any{
				[]any{
					[]any{"customer_rank", ">", 0},
				},
			},
			"kwargs": map[string]any{
				"fields": fields,
				"offset": offset,
				"limit":  limit,
			},
		},
		"id": 2,
	}

	resp, err := odoo.OdooManager.NewRequest("POST", "web/dataset/call_kw", payload)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var rpcResp struct {
		Result []map[string]any `json:"result"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, err
	}

	return rpcResp.Result, nil
}

// cleanCustomer cleans and transforms a customer from Odoo format to our format
func cleanCustomer(c map[string]any) map[string]any {
	// Helper functions
	getInt := func(v any) int {
		if v == nil {
			return 0
		}
		if f, ok := v.(float64); ok {
			return int(f)
		}
		return 0
	}

	getFloat := func(v any) float64 {
		if v == nil {
			return 0.0
		}
		if f, ok := v.(float64); ok {
			return f
		}
		return 0.0
	}

	getString := func(v any) string {
		if v == nil {
			return "NA"
		}
		return fmt.Sprintf("%v", v)
	}

	getStringOrNA := func(v any) string {
		if v == nil {
			return "NA"
		}
		str := fmt.Sprintf("%v", v)
		if str == "" {
			return "NA"
		}
		return str
	}

	// Check on_hold flag
	creditHold := getString(c["credit_hold"])
	onHoldFlag := strings.ToLower(creditHold) == "true" || creditHold == "1"

	// Process property_payment_term_id
	paymentTermID := "NA"
	if pt, ok := c["property_payment_term_id"].([]any); ok && len(pt) > 1 {
		paymentTermID = getString(pt[1])
	}

	// Process user_id
	userID := "NA"
	if uid, ok := c["user_id"].([]any); ok && len(uid) > 1 {
		userID = getString(uid[0])
	} else if uid := c["user_id"]; uid != nil {
		userID = getString(uid)
	}

	// Process property_product_pricelist
	defaultPricelist := "1"
	if ppl, ok := c["property_product_pricelist"].([]any); ok && len(ppl) > 1 {
		defaultPricelist = getString(ppl[0])
	}

	customerID := getInt(c["id"])

	cleaned := map[string]any{
		"id":                         fmt.Sprintf("%d", customerID),
		"customer_id":                customerID,
		"x_studio_account_number":    getStringOrNA(c["x_studio_account_number"]),
		"display_name":               getStringOrNA(c["display_name"]),
		"email":                      getStringOrNA(c["email"]),
		"phone":                      getStringOrNA(c["phone"]),
		"city":                       getStringOrNA(c["city"]),
		"zip":                        getStringOrNA(c["zip"]),
		"property_payment_term_id":   paymentTermID,
		"hold_delivery_till_payment": getString(c["hold_delivery_till_payment"]),
		"credit_hold":                getString(c["credit_hold"]),
		"has_overdue_by_x_days":      getInt(c["has_overdue_by_x_days"]),
		"total_overdue":              getFloat(c["total_overdue"]),
		"credit":                     getFloat(c["credit"]),
		"days_sales_outstanding":     getInt(c["days_sales_outstanding"]),
		"user_id":                    userID,
		"default_pricelist":          defaultPricelist,
		"on_hold":                    onHoldFlag,
	}

	return cleaned
}

// ensureCustomersSchema ensures the Typesense customers collection schema exists
func ensureCustomersSchema(ctx context.Context) error {
	// Try to retrieve the collection
	_, err := typesenseutil.TypesenseClient.Collection("customers").Retrieve(ctx)
	if err == nil {
		log.Println("✅ Customers collection exists")
		return nil
	}

	// Collection doesn't exist, create it
	log.Println("⚠️  Customers collection missing, creating...")

	sortTrue := true
	schema := &api.CollectionSchema{
		Name: "customers",
		Fields: []api.Field{
			{Name: "id", Type: "string"},
			{Name: "customer_id", Type: "int32"},
			{Name: "x_studio_account_number", Type: "string"},
			{Name: "display_name", Type: "string", Sort: &sortTrue},
			{Name: "email", Type: "string"},
			{Name: "property_payment_term_id", Type: "string"},
			{Name: "phone", Type: "string"},
			{Name: "zip", Type: "string", Facet: &sortTrue},
			{Name: "hold_delivery_till_payment", Type: "string", Facet: &sortTrue},
			{Name: "credit_hold", Type: "string"},
			{Name: "has_overdue_by_x_days", Type: "int32", Sort: &sortTrue},
			{Name: "total_overdue", Type: "float", Sort: &sortTrue},
			{Name: "city", Type: "string", Facet: &sortTrue},
			{Name: "credit", Type: "float", Sort: &sortTrue},
			{Name: "days_sales_outstanding", Type: "int32", Sort: &sortTrue},
			{Name: "user_id", Type: "string", Facet: &sortTrue},
			{Name: "default_pricelist", Type: "string"},
			{Name: "on_hold", Type: "bool", Facet: &sortTrue},
		},
	}

	_, err = typesenseutil.TypesenseClient.Collections().Create(ctx, schema)
	if err != nil {
		return fmt.Errorf("failed to create customers collection: %w", err)
	}

	log.Println("✅ Customers collection created")
	return nil
}
