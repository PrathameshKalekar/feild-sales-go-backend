package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/PrathameshKalekar/field-sales-go-backend/internal/odoo"
	redisutil "github.com/PrathameshKalekar/field-sales-go-backend/internal/redis"
	"github.com/hibiken/asynq"
)

func HandleSyncPricelistsTask(ctx context.Context, t *asynq.Task) error {
	defer MarkCoreTaskCompletion(ctx)
	log.Println("🔄 Starting pricelist sync...")
	startTime := time.Now()

	offset, limit := 0, 1000
	totalCount := 0
	batchNo := 0

	// Fetch pricelist master
	pricelistMaster, err := fetchPricelistMaster()
	if err != nil {
		log.Printf("❌ Failed to fetch pricelist master: %v", err)
		return err
	}

	// Filter out excluded pricelist IDs
	excludedIDs := map[int]bool{675: true, 678: true, 683: true, 674: true, 9: true, 10: true, 11: true, 12: true, 21: true}
	filteredMaster := make(map[int]string)
	for id, name := range pricelistMaster {
		if !excludedIDs[id] {
			filteredMaster[id] = name
		}
	}

	// Save pricelist master to Redis
	if len(filteredMaster) > 0 {
		masterJSON, err := json.Marshal(filteredMaster)
		if err != nil {
			log.Printf("❌ Failed to marshal pricelist master: %v", err)
		} else {
			if err := redisutil.RedisClient.Set(ctx, "pricelist:master", masterJSON, 0).Err(); err != nil {
				log.Printf("❌ Redis couldn't save pricelist master: %v", err)
			}
		}
	}

	// Fetch and process pricelist items in batches
	for {
		batch, err := fetchPricelists(offset, limit)
		if err != nil {
			log.Printf("❌ Failed to fetch pricelists batch: %v", err)
			return err
		}

		if len(batch) == 0 {
			break
		}

		// Use Redis pipeline for batch operations
		pipe := redisutil.RedisClient.Pipeline()
		processedInBatch := 0

		for _, prConfig := range batch {
			redisKey, mapping, err := processPricelistItem(prConfig)
			if err != nil {
				log.Printf("⚠️  Skipping invalid record at offset %d: %v", offset, err)
				continue
			}

			// Convert mapping to map[string]string for HSET
			hsetMap := make(map[string]string)
			for k, v := range mapping {
				hsetMap[k] = fmt.Sprintf("%v", v)
			}

			pipe.HSet(ctx, redisKey, hsetMap)
			processedInBatch++
		}

		// Execute pipeline
		_, err = pipe.Exec(ctx)
		if err != nil {
			log.Printf("❌ Redis pipeline failed on batch %d: %v", batchNo+1, err)
			time.Sleep(2 * time.Second)
			offset += limit
			continue
		}

		totalCount += processedInBatch
		batchNo++
		log.Printf("✅ Pricelist - Batch %d done | Total synced: %d", batchNo, totalCount)

		offset += limit
	}

	duration := time.Since(startTime)
	log.Printf("✅ Pricelist - Sync complete — %d records in %.2fs", totalCount, duration.Seconds())
	return nil
}

// fetchPricelistMaster fetches the pricelist master data from Odoo
func fetchPricelistMaster() (map[int]string, error) {
	payload := map[string]any{
		"jsonrpc": "2.0",
		"method":  "call",
		"params": map[string]any{
			"model":  "product.pricelist",
			"method": "search_read",
			"args": []any{
				[]any{
					[]any{"active", "=", true},
				},
			},
			"kwargs": map[string]any{
				"fields": []string{"id", "name"},
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

	pricelistMaster := make(map[int]string)
	for _, obj := range rpcResp.Result {
		id := int(obj["id"].(float64))
		name := obj["name"].(string)
		pricelistMaster[id] = name
	}

	return pricelistMaster, nil
}

// fetchPricelists fetches pricelist items from Odoo in batches
func fetchPricelists(offset, limit int) ([]map[string]any, error) {
	payload := map[string]any{
		"jsonrpc": "2.0",
		"method":  "call",
		"params": map[string]any{
			"model":  "product.pricelist.item",
			"method": "search_read",
			"args":   []any{[]any{}},
			"kwargs": map[string]any{
				"fields": []string{
					"pricelist_id", "categ_id", "product_tmpl_id",
					"base_pricelist_id", "applied_on", "base",
					"price_discount", "percent_price",
				},
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

// processPricelistItem processes a single pricelist item and returns Redis key and mapping
func processPricelistItem(prConfig map[string]any) (string, map[string]any, error) {
	// Extract pricelist_id
	var pricelistID any
	if pid, ok := prConfig["pricelist_id"].([]any); ok && len(pid) > 0 {
		pricelistID = pid[0]
	} else {
		pricelistID = nil
	}

	// Extract category
	var categoryID any
	if cat, ok := prConfig["categ_id"].([]any); ok && len(cat) > 0 {
		categoryID = cat[0]
	} else {
		categoryID = nil
	}

	// Extract product
	var productID any
	if prod, ok := prConfig["product_tmpl_id"].([]any); ok && len(prod) > 0 {
		productID = prod[0]
	} else {
		productID = nil
	}

	// Extract base_pricelist_id
	var basePricelistID any
	if bpl, ok := prConfig["base_pricelist_id"].([]any); ok && len(bpl) > 0 {
		basePricelistID = bpl[0]
	} else {
		basePricelistID = nil
	}

	// Get price_discount and percent_price
	priceDiscount := 0.0
	if pd, ok := prConfig["price_discount"].(float64); ok {
		priceDiscount = pd
	}

	percentPrice := 0.0
	if pp, ok := prConfig["percent_price"].(float64); ok {
		percentPrice = pp
	}

	// Determine discount value (use price_discount if not 0, otherwise percent_price)
	discount := priceDiscount
	if discount == 0 {
		discount = percentPrice
	}

	// Build Redis key
	pricelistIDStr := "null"
	if pricelistID != nil {
		pricelistIDStr = strconv.FormatFloat(pricelistID.(float64), 'f', -1, 64)
	}

	categoryIDStr := "null"
	if categoryID != nil {
		categoryIDStr = strconv.FormatFloat(categoryID.(float64), 'f', -1, 64)
	}

	productIDStr := "null"
	if productID != nil {
		productIDStr = strconv.FormatFloat(productID.(float64), 'f', -1, 64)
	}

	redisKey := fmt.Sprintf("pricelist:%s:category:%s:product:%s", pricelistIDStr, categoryIDStr, productIDStr)

	// Build mapping
	basePricelistIDStr := ""
	if basePricelistID != nil {
		basePricelistIDStr = strconv.FormatFloat(basePricelistID.(float64), 'f', -1, 64)
	}

	base := ""
	if b, ok := prConfig["base"].(string); ok {
		base = b
	}

	mapping := map[string]any{
		"base_pricelist_id": basePricelistIDStr,
		"base":              base,
		"discount":          strconv.FormatFloat(discount, 'f', -1, 64),
	}

	return redisKey, mapping, nil
}
