package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/PrathameshKalekar/field-sales-go-backend/internal/config"
	"github.com/PrathameshKalekar/field-sales-go-backend/internal/odoo"
	redisutil "github.com/PrathameshKalekar/field-sales-go-backend/internal/redis"
	typesenseutil "github.com/PrathameshKalekar/field-sales-go-backend/internal/typesense"
	"github.com/hibiken/asynq"
	"github.com/typesense/typesense-go/v4/typesense/api"
)

func HandleSyncInvoicesAndLinesTask(ctx context.Context, t *asynq.Task) error {
	log.Println("🔄 Starting unified invoice + invoice lines sync...")

	// Load last sync timestamp
	lastSyncKey := "invoices:last_sync_datetime"
	lastSyncStr, _ := redisutil.RedisClient.Get(ctx, lastSyncKey).Result()

	var lastSync time.Time
	if lastSyncStr != "" {
		existingIDsRaw, _ := redisutil.RedisClient.Get(ctx, "invoices:all_ids").Result()
		if existingIDsRaw != "" {
			var existingIDs []int
			if err := json.Unmarshal([]byte(existingIDsRaw), &existingIDs); err == nil && len(existingIDs) != 0 {
				parsedTime, err := time.Parse("2006-01-02T15:04:05", lastSyncStr)
				if err != nil {
					parsedTime, err = time.Parse(time.RFC3339, lastSyncStr)
				}
				if err == nil {
					lastSync = parsedTime
					log.Printf("⏳ Last invoice sync: %s", lastSync.Format("2006-01-02 15:04:05"))
				} else {
					lastSync = time.Now().UTC().AddDate(0, 0, -180)
					log.Println("🆕 First-time invoice sync → fetching last 180 days")
				}
			} else {
				lastSync = time.Now().UTC().AddDate(0, 0, -180)
				log.Println("🆕 First-time invoice sync → fetching last 180 days")
			}
		} else {
			lastSync = time.Now().UTC().AddDate(0, 0, -180)
			log.Println("🆕 First-time invoice sync → fetching last 180 days")
		}
	} else {
		lastSync = time.Now().UTC().AddDate(0, 0, -180)
		log.Println("🆕 First-time invoice sync → fetching last 180 days")
	}

	fields := []string{
		"id", "name", "invoice_date", "partner_id",
		"amount_total", "payment_state",
		"x_studio_related_field_6nn_1ihffsbf0",
	}

	allInvoiceIDs := []int{}
	allInvoices := []map[string]any{}
	offset, limit, pageNum := 0, 2000, 1
	maxInvoiceDate := lastSync

	// Fetch invoices
	domain := []any{
		[]any{"move_type", "=", "out_invoice"},
		[]any{"state", "=", "posted"},
		[]any{"invoice_date", ">", lastSync.Format("2006-01-02")},
	}

	for {
		payload := map[string]any{
			"jsonrpc": "2.0",
			"method":  "call",
			"params": map[string]any{
				"model":  "account.move",
				"method": "search_read",
				"args":   []any{domain},
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
			log.Printf("❌ Failed to fetch invoices batch: %v", err)
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

		invoices := rpcResp.Result
		if len(invoices) == 0 {
			break
		}

		pageDocs := []map[string]any{}

		for _, inv := range invoices {
			invoiceID := 0
			if id, ok := inv["id"].(float64); ok {
				invoiceID = int(id)
			}

			// Extract fields
			invDate := ""
			if inv["invoice_date"] != nil {
				invDate = fmt.Sprintf("%v", inv["invoice_date"])
			}
			dtInv, _ := time.Parse("2006-01-02T15:04:05", strings.Replace(invDate, " ", "T", 1))
			tsInv := int(dtInv.Unix())

			partnerName := "NA"
			partnerID := 0
			if partner, ok := inv["partner_id"].([]any); ok && len(partner) >= 2 {
				if id, ok := partner[0].(float64); ok {
					partnerID = int(id)
				}
				partnerName = fmt.Sprintf("%v", partner[1])
			}

			salesperson := "Unknown"
			if sp, ok := inv["x_studio_related_field_6nn_1ihffsbf0"].([]any); ok && len(sp) > 0 {
				salesperson = fmt.Sprintf("%v", sp[0])
			} else if sp := inv["x_studio_related_field_6nn_1ihffsbf0"]; sp != nil {
				salesperson = fmt.Sprintf("%v", sp)
			}

			name := "NA"
			if inv["name"] != nil {
				name = fmt.Sprintf("%v", inv["name"])
			}

			amountTotal := 0.0
			if amt, ok := inv["amount_total"].(float64); ok {
				amountTotal = amt
			}

			paymentState := "NA"
			if inv["payment_state"] != nil {
				paymentState = fmt.Sprintf("%v", inv["payment_state"])
			}

			invoiceDoc := map[string]any{
				"id":              fmt.Sprintf("%d", invoiceID),
				"name":            name,
				"invoice_date":    invDate,
				"invoice_date_ts": tsInv,
				"partner_id":      partnerID,
				"partner_name":    partnerName,
				"salesperson":     salesperson,
				"amount_total":    amountTotal,
				"payment_state":   paymentState,
				"pdf_url":         fmt.Sprintf("%s/report/pdf/account.report_invoice/%d", config.ConfigGlobal.OdooURL, invoiceID),
			}

			// Save invoice header
			invoiceKey := fmt.Sprintf("invoices:%d", invoiceID)
			hsetMap := make(map[string]string)
			for k, v := range invoiceDoc {
				hsetMap[k] = fmt.Sprintf("%v", v)
			}
			redisutil.RedisClient.HSet(ctx, invoiceKey, hsetMap)

			allInvoiceIDs = append(allInvoiceIDs, invoiceID)
			allInvoices = append(allInvoices, invoiceDoc)
			pageDocs = append(pageDocs, invoiceDoc)

			// Track max invoice_date for incremental sync
			if dtInv.After(maxInvoiceDate) {
				maxInvoiceDate = dtInv
			}
		}

		log.Printf("[INVOICE_SYNC][PAGE] Invoices page %d synced | Records: %d", pageNum, len(pageDocs))

		offset += limit
		pageNum++
	}

	// Merge with existing invoice IDs
	existingIDsRaw, _ := redisutil.RedisClient.Get(ctx, "invoices:all_ids").Result()
	existingIDs := make(map[int]bool)
	if existingIDsRaw != "" {
		var ids []int
		if err := json.Unmarshal([]byte(existingIDsRaw), &ids); err == nil {
			for _, id := range ids {
				existingIDs[id] = true
			}
		}
	}

	// Add new IDs
	for _, id := range allInvoiceIDs {
		existingIDs[id] = true
	}

	// Convert back to list
	mergedIDs := make([]int, 0, len(existingIDs))
	for id := range existingIDs {
		mergedIDs = append(mergedIDs, id)
	}

	// Save back
	mergedIDsJSON, _ := json.Marshal(mergedIDs)
	redisutil.RedisClient.Set(ctx, "invoices:all_ids", mergedIDsJSON, 0)
	log.Printf("✅Total invoices stored: %d", len(mergedIDs))

	// Fetch invoice lines
	log.Printf("🔄 Fetching invoice lines for %d invoices...", len(allInvoiceIDs))

	batchSize := 2000
	for i := 0; i < len(allInvoiceIDs); i += batchSize {
		end := i + batchSize
		if end > len(allInvoiceIDs) {
			end = len(allInvoiceIDs)
		}
		batch := allInvoiceIDs[i:end]

		req := map[string]any{
			"jsonrpc": "2.0",
			"method":  "call",
			"params": map[string]any{
				"model":  "account.move.line",
				"method": "search_read",
				"args": []any{
					[]any{[]any{"move_id", "in", batch}},
				},
				"kwargs": map[string]any{
					"fields": []string{"move_id", "product_id", "quantity", "price_total"},
				},
			},
			"id": 2,
		}

		resp, err := odoo.OdooManager.NewRequest("POST", "web/dataset/call_kw", req)
		if err != nil {
			log.Printf("❌ Failed to fetch invoice lines batch: %v", err)
			continue
		}

		var lineResp struct {
			Result []map[string]any `json:"result"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&lineResp); err != nil {
			resp.Body.Close()
			log.Printf("❌ Failed to decode invoice lines response: %v", err)
			continue
		}
		resp.Body.Close()

		lines := lineResp.Result

		// Group lines by invoice ID
		bucket := make(map[int][]map[string]any)
		for _, l := range lines {
			invID := 0
			if moveID, ok := l["move_id"].([]any); ok && len(moveID) > 0 {
				if id, ok := moveID[0].(float64); ok {
					invID = int(id)
				}
			}

			prodName := "Unknown"
			prodID := 0
			if prod, ok := l["product_id"].([]any); ok && len(prod) >= 2 {
				if id, ok := prod[0].(float64); ok {
					prodID = int(id)
				}
				prodName = fmt.Sprintf("%v", prod[1])
			}

			quantity := 0.0
			if qty, ok := l["quantity"].(float64); ok {
				quantity = qty
			}

			priceTotal := 0.0
			if pt, ok := l["price_total"].(float64); ok {
				priceTotal = pt
			}

			bucket[invID] = append(bucket[invID], map[string]any{
				"product":     prodName,
				"product_id":  prodID,
				"quantity":    quantity,
				"price_total": priceTotal,
			})
		}

		// Save invoice lines
		for invID, lineList := range bucket {
			lineJSON, _ := json.Marshal(lineList)
			redisutil.RedisClient.Set(ctx, fmt.Sprintf("invoice_lines:%d", invID), lineJSON, 0)
		}
		log.Printf("Invoice lines fetched for batch size - %d", len(batch))
	}

	// Typesense
	if err := ensureInvoicesSchema(ctx); err != nil {
		log.Printf("❌ Failed to ensure schema: %v", err)
		return err
	}

	if len(allInvoices) > 0 {
		documents := make([]any, len(allInvoices))
		for i, inv := range allInvoices {
			documents[i] = inv
		}

		action := api.IndexAction("upsert")
		_, err := typesenseutil.TypesenseClient.Collection("invoices").Documents().Import(ctx, documents, &api.ImportDocumentsParams{
			Action: &action,
		})
		if err != nil {
			log.Printf("❌ Failed to import invoices to Typesense: %v", err)
		}
	} else {
		log.Println("ℹ️  No new invoices to import into Typesense")
	}

	// Save new sync timestamp
	redisutil.RedisClient.Set(ctx, lastSyncKey, maxInvoiceDate.Format("2006-01-02T15:04:05"), 0)

	log.Println("✅ All invoices + invoice lines synced successfully.")
	MarkOrderTaskCompletion(ctx)
	return nil
}

// ensureInvoicesSchema ensures the Typesense invoices collection schema exists
func ensureInvoicesSchema(ctx context.Context) error {
	// Try to retrieve the collection
	_, err := typesenseutil.TypesenseClient.Collection("invoices").Retrieve(ctx)
	if err == nil {
		return nil
	}

	// Collection doesn't exist, create it
	sortTrue := true
	defaultSortingField := "invoice_date_ts"
	schema := &api.CollectionSchema{
		Name: "invoices",
		Fields: []api.Field{
			{Name: "id", Type: "string"},
			{Name: "name", Type: "string"},
			{Name: "invoice_date", Type: "string"},
			{Name: "invoice_date_ts", Type: "int64", Facet: &sortTrue, Sort: &sortTrue},
			{Name: "partner_id", Type: "int32", Facet: &sortTrue},
			{Name: "partner_name", Type: "string", Facet: &sortTrue},
			{Name: "salesperson", Type: "string", Facet: &sortTrue},
			{Name: "amount_total", Type: "float", Sort: &sortTrue},
			{Name: "payment_state", Type: "string", Facet: &sortTrue},
			{Name: "pdf_url", Type: "string"},
		},
		DefaultSortingField: &defaultSortingField,
	}

	_, err = typesenseutil.TypesenseClient.Collections().Create(ctx, schema)
	if err != nil {
		return fmt.Errorf("failed to create invoices collection: %w", err)
	}

	return nil
}
