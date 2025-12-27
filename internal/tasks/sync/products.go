package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/PrathameshKalekar/field-sales-go-backend/internal/odoo"
	redisutil "github.com/PrathameshKalekar/field-sales-go-backend/internal/redis"
	typesenseutil "github.com/PrathameshKalekar/field-sales-go-backend/internal/typesense"
	"github.com/hibiken/asynq"
	"github.com/typesense/typesense-go/v4/typesense/api"
)

func HandleSyncProductsTask(ctx context.Context, task *asynq.Task) error {
	defer MarkCoreTaskCompletion(ctx)
	log.Println("🔄 Starting products sync...")

	// First fetch all product categories
	if err := GetProductCategories(ctx); err != nil {
		log.Printf("❌ Failed to fetch product categories: %v", err)
		return err
	}

	allProducts := []map[string]any{}
	offset, limit, pageNum := 0, 250, 1
	letterPattern := regexp.MustCompile(`[A-Za-z]`)

	// Fetch auxiliary data
	productTags, err := fetchProductTags(ctx)
	if err != nil {
		log.Printf("❌ Failed to fetch product tags: %v", err)
		return err
	}

	productTaxes, err := fetchProductTaxes(ctx)
	if err != nil {
		log.Printf("❌ Failed to fetch product taxes: %v", err)
		return err
	}

	caseBarcodes, err := fetchCaseBarcode(ctx)
	if err != nil {
		log.Printf("❌ Failed to fetch case barcodes: %v", err)
		return err
	}

	// Get product category parent path from Redis
	parentPathData, err := redisutil.RedisClient.Get(ctx, "product_cat_parent_path").Result()
	if err != nil {
		log.Printf("❌ Failed to get product_cat_parent_path from Redis: %v", err)
		return err
	}

	var idToParentPath map[string][]int
	if err := json.Unmarshal([]byte(parentPathData), &idToParentPath); err != nil {
		log.Printf("❌ Failed to unmarshal parent path: %v", err)
		return err
	}

	// Fetch products in batches
	for {
		batch, err := fetchProducts(ctx, offset, limit)
		if err != nil {
			log.Printf("❌ Failed to fetch products batch: %v", err)
			return err
		}

		if len(batch) == 0 {
			break
		}

		batchProducts := []map[string]any{}
		for _, product := range batch {
			// Skip products with letters in default_code
			defaultCode, _ := product["default_code"].(string)
			if letterPattern.MatchString(defaultCode) {
				continue
			}

			cleanedProduct := cleanProduct(product, productTags, productTaxes, caseBarcodes, idToParentPath)
			batchProducts = append(batchProducts, cleanedProduct)
			allProducts = append(allProducts, cleanedProduct)
		}

		// Save paginated data in Redis
		batchJSON, err := json.Marshal(batchProducts)
		if err != nil {
			log.Printf("❌ Failed to marshal batch products: %v", err)
			return err
		}

		if err := redisutil.RedisClient.Set(ctx, fmt.Sprintf("products:%d", pageNum), batchJSON, 0).Err(); err != nil {
			log.Printf("❌ Failed to save batch to Redis: %v", err)
			return err
		}

		log.Printf("📦 Products - Batch %d done", pageNum)

		offset += limit
		pageNum++
	}

	// Save product count in Redis
	if err := redisutil.RedisClient.Set(ctx, "products:total", len(allProducts), 0).Err(); err != nil {
		log.Printf("❌ Failed to save product count: %v", err)
		return err
	}

	// Ensure Typesense schema exists
	if err := ensureProductsSchema(ctx); err != nil {
		log.Printf("❌ Failed to ensure schema: %v", err)
		return err
	}

	// Wipe old docs and bulk import
	if len(allProducts) > 0 {
		// Delete old documents
		filterBy := "product_id:>0"
		_, err := typesenseutil.TypesenseClient.Collection("products").Documents().Delete(ctx, &api.DeleteDocumentsParams{
			FilterBy: &filterBy,
		})
		if err != nil {
			log.Printf("⚠️  Could not delete old documents (collection may not exist): %v", err)
		}

		// Convert to []any for Import
		documents := make([]any, len(allProducts))
		for i, p := range allProducts {
			documents[i] = p
		}

		// Bulk import
		action := api.IndexAction("upsert")
		importResult, err := typesenseutil.TypesenseClient.Collection("products").Documents().Import(ctx, documents, &api.ImportDocumentsParams{
			Action: &action,
		})
		if err != nil {
			log.Printf("❌ Failed to import products to Typesense: %v", err)
			return err
		}

		log.Printf("✅ Synced %d products into Redis + Typesense. Imported %d documents", len(allProducts), len(importResult))
	}

	log.Println("✅ Products sync completed!")
	return nil
}

type ProductCategory struct {
	ID         int    `json:"id"`
	Name       string `json:"name"`
	ParentID   *int   `json:"parent_id"`
	ParentPath []int  `json:"parent_path"`
}

func GetProductCategories(ctx context.Context) error {

	fmt.Println("[PRODUCT_CATEGORIES][START] Fetching product categories from Odoo")

	const productCatKey = "product_categories"
	const parentPathKey = "product_cat_parent_path"

	payload := map[string]any{
		"jsonrpc": "2.0",
		"method":  "call",
		"params": map[string]any{
			"model":  "product.public.category",
			"method": "search_read",
			"args":   []any{[]any{}},
			"kwargs": map[string]any{
				"fields":  []string{"id", "name", "parent_id", "parent_path"},
				"context": map[string]any{"lang": "en_GB"},
			},
		},
		"id": 2,
	}

	resp, err := odoo.OdooManager.NewRequest("POST", "web/dataset/call_kw", payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var rpcResp struct {
		Result []map[string]any `json:"result"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return err
	}

	categories := make([]ProductCategory, 0, len(rpcResp.Result))
	parentPathMap := make(map[int][]int)

	for _, row := range rpcResp.Result {
		var parentID *int

		if p, ok := row["parent_id"].([]any); ok && len(p) > 0 {
			id := int(p[0].(float64))
			parentID = &id
		}

		pathStr := row["parent_path"].(string)
		pathIDs := parseParentPath(pathStr)

		cat := ProductCategory{
			ID:         int(row["id"].(float64)),
			Name:       row["name"].(string),
			ParentID:   parentID,
			ParentPath: pathIDs,
		}

		categories = append(categories, cat)
		parentPathMap[cat.ID] = pathIDs
	}

	// Cache in Redis
	if err := setJSON(ctx, productCatKey, categories); err != nil {
		return err
	}

	if err := setJSON(ctx, parentPathKey, parentPathMap); err != nil {
		return err
	}

	fmt.Println("[PRODUCT_CATEGORIES][SUCCESS] Categories cached in Redis")
	return nil
}

func parseParentPath(path string) []int {
	path = strings.Trim(path, "/")
	if path == "" {
		return []int{}
	}

	parts := strings.Split(path, "/")
	ids := make([]int, 0, len(parts))

	for _, p := range parts {
		id, _ := strconv.Atoi(p)
		ids = append(ids, id)
	}

	return ids
}
func setJSON(ctx context.Context, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return redisutil.RedisClient.Set(ctx, key, data, 0).Err()
}

// fetchProductTags fetches product tags from Odoo and caches them in Redis
func fetchProductTags(ctx context.Context) (map[int]string, error) {
	payload := map[string]any{
		"jsonrpc": "2.0",
		"method":  "call",
		"params": map[string]any{
			"model":  "product.tag",
			"method": "search_read",
			"args":   []any{[]any{}},
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

	productTags := make(map[int]string)
	for _, obj := range rpcResp.Result {
		id := int(obj["id"].(float64))
		name := obj["name"].(string)
		productTags[id] = name
	}

	// Cache in Redis
	if len(productTags) > 0 {
		redisutil.RedisClient.Del(ctx, "product_tags")
		for _, tag := range productTags {
			redisutil.RedisClient.LPush(ctx, "product_tags", tag)
		}
	}

	return productTags, nil
}

// fetchProductTaxes fetches product taxes from Odoo
func fetchProductTaxes(ctx context.Context) (map[int][2]any, error) {
	payload := map[string]any{
		"jsonrpc": "2.0",
		"method":  "call",
		"params": map[string]any{
			"model":  "account.tax",
			"method": "search_read",
			"args":   []any{[]any{}},
			"kwargs": map[string]any{
				"fields": []string{"id", "name", "amount"},
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

	productTaxes := make(map[int][2]any)
	for _, obj := range rpcResp.Result {
		id := int(obj["id"].(float64))
		name := obj["name"].(string)
		amount := obj["amount"].(float64)
		productTaxes[id] = [2]any{name, amount}
	}

	return productTaxes, nil
}

// fetchCaseBarcode fetches product packaging barcodes from Odoo
func fetchCaseBarcode(ctx context.Context) (map[int]string, error) {
	payload := map[string]any{
		"jsonrpc": "2.0",
		"method":  "call",
		"params": map[string]any{
			"model":  "product.packaging",
			"method": "search_read",
			"args":   []any{[]any{}},
			"kwargs": map[string]any{
				"fields": []string{"product_id", "barcode"},
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

	caseBarcode := make(map[int]string)
	for _, obj := range rpcResp.Result {
		var productID int
		pid := obj["product_id"]
		if pidList, ok := pid.([]any); ok && len(pidList) > 0 {
			productID = int(pidList[0].(float64))
		} else {
			continue
		}

		barcode := obj["barcode"]
		if barcode == nil || barcode == false || barcode == "" {
			continue
		}

		barcodeStr := strings.TrimSpace(fmt.Sprintf("%v", barcode))
		if barcodeStr != "" {
			caseBarcode[productID] = barcodeStr
		}
	}

	// Store in Redis
	if len(caseBarcode) > 0 {
		redisutil.RedisClient.Del(ctx, "case_barcode")
		for _, bc := range caseBarcode {
			redisutil.RedisClient.LPush(ctx, "case_barcode", bc)
		}
	}

	return caseBarcode, nil
}

// fetchProducts fetches products from Odoo in batches
func fetchProducts(ctx context.Context, offset, limit int) ([]map[string]any, error) {
	fields := []string{
		"id", "product_tmpl_id", "categ_id", "name", "list_price", "standard_price",
		"x_studio_image_url", "x_studio_msl_to_customer", "x_studio_msl_to_cn",
		"x_studio_storage", "uom_id", "qty_available", "outgoing_qty", "product_tag_ids",
		"public_categ_ids", "barcode", "x_studio_units_per_case", "x_studio_rrp",
		"taxes_id", "default_code", "website_sequence", "x_studio_brand_name", "weight",
	}

	payload := map[string]any{
		"jsonrpc": "2.0",
		"method":  "call",
		"params": map[string]any{
			"model":  "product.product",
			"method": "search_read",
			"args": []any{
				[]any{
					[]any{"list_price", ">", 0.5},
					[]any{"sale_ok", "=", true},
					[]any{"active", "=", true},
				},
			},
			"kwargs": map[string]any{
				"fields": fields,
				"offset": offset,
				"limit":  limit,
				"order":  "website_sequence asc",
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

	// Convert []map[string]any to []map[string]any
	result := make([]map[string]any, len(rpcResp.Result))
	for i, item := range rpcResp.Result {
		result[i] = make(map[string]any)
		for k, v := range item {
			result[i][k] = v
		}
	}

	return result, nil
}

// cleanProduct cleans and transforms a product from Odoo format to our format
func cleanProduct(product map[string]any, productTags map[int]string, productTaxes map[int][2]any, caseBarcodes map[int]string, idToParentPath map[string][]int) map[string]any {
	// Helper to extract ID from [id, name] format
	getID := func(v any) int {
		if list, ok := v.([]any); ok && len(list) >= 2 {
			return int(list[0].(float64))
		}
		return 0
	}

	// Helper to extract name from [id, name] format
	getName := func(v any) string {
		if list, ok := v.([]any); ok && len(list) >= 2 {
			return list[1].(string)
		}
		return ""
	}

	// Helper to get int value
	getInt := func(v any) int {
		if v == nil {
			return 0
		}
		if f, ok := v.(float64); ok {
			return int(f)
		}
		return 0
	}

	// Helper to get float value
	getFloat := func(v any) float64 {
		if v == nil {
			return 0.0
		}
		if f, ok := v.(float64); ok {
			return f
		}
		return 0.0
	}

	// Helper to get string value
	getString := func(v any) string {
		if v == nil {
			return ""
		}
		return fmt.Sprintf("%v", v)
	}

	productID := getInt(product["id"])
	templateID := getID(product["product_tmpl_id"])
	categID := getID(product["categ_id"])
	if categID == 0 {
		categID = -1
	}

	// Process image URL
	imageURL := getString(product["x_studio_image_url"])
	imageURL = strings.ReplaceAll(imageURL, "1920", "128")

	// Process barcode
	var barcode any
	barcodeVal := product["barcode"]
	if barcodeVal != nil && barcodeVal != false && barcodeVal != "" && barcodeVal != "False" {
		barcode = getString(barcodeVal)
	} else {
		barcode = nil
	}

	// Process product tags
	tagIDs := []int{}
	if tags, ok := product["product_tag_ids"].([]any); ok {
		for _, tagID := range tags {
			tagIDs = append(tagIDs, int(tagID.(float64)))
		}
	}
	productTagsList := []string{}
	for _, tagID := range tagIDs {
		if tag, exists := productTags[tagID]; exists {
			productTagsList = append(productTagsList, tag)
		}
	}

	// Process taxes
	taxIDs := []int{}
	if taxes, ok := product["taxes_id"].([]any); ok {
		for _, taxID := range taxes {
			taxIDs = append(taxIDs, int(taxID.(float64)))
		}
	}
	taxesList := []string{}
	var taxPercent float64
	if len(taxIDs) > 0 {
		if taxData, exists := productTaxes[taxIDs[0]]; exists {
			taxesList = append(taxesList, taxData[0].(string))
			taxPercent = taxData[1].(float64)
		}
	}

	// Process categories
	publicCategIDs := []int{}
	if categs, ok := product["public_categ_ids"].([]any); ok {
		for _, categID := range categs {
			publicCategIDs = append(publicCategIDs, int(categID.(float64)))
		}
	}
	categoriesSet := make(map[int]bool)
	for _, categID := range publicCategIDs {
		if paths, exists := idToParentPath[fmt.Sprintf("%d", categID)]; exists {
			for _, pathID := range paths {
				categoriesSet[pathID] = true
			}
		}
	}
	categories := []int{}
	for catID := range categoriesSet {
		categories = append(categories, catID)
	}

	// Get case barcode
	caseBarcode := ""
	if cb, exists := caseBarcodes[productID]; exists {
		caseBarcode = cb
	}

	cleaned := map[string]any{
		"id":               fmt.Sprintf("%d", productID),
		"product_id":       productID,
		"template_id":      templateID,
		"categ_id":         categID,
		"name":             getString(product["name"]),
		"list_price":       getFloat(product["list_price"]),
		"standard_price":   getFloat(product["standard_price"]),
		"image_url":        imageURL,
		"msl":              getInt(product["x_studio_msl_to_customer"]),
		"bsl":              getInt(product["x_studio_msl_to_cn"]),
		"storage":          getString(product["x_studio_storage"]),
		"uom":              getName(product["uom_id"]),
		"qty_available":    getInt(product["qty_available"]) - getInt(product["outgoing_qty"]),
		"outgoing_qty":     getInt(product["outgoing_qty"]),
		"product_tags":     productTagsList,
		"barcode":          barcode,
		"units_per_case":   getInt(product["x_studio_units_per_case"]),
		"rrp":              getFloat(product["x_studio_rrp"]),
		"sku":              getString(product["default_code"]),
		"taxes":            taxesList,
		"tax_percent":      taxPercent,
		"categories":       categories,
		"website_sequence": getInt(product["website_sequence"]),
		"brand":            getString(product["x_studio_brand_name"]),
		"weight":           getFloat(product["weight"]),
		"case_barcode":     caseBarcode,
	}

	return cleaned
}

// ensureProductsSchema ensures the Typesense products collection schema exists
func ensureProductsSchema(ctx context.Context) error {
	// Try to retrieve the collection
	_, err := typesenseutil.TypesenseClient.Collection("products").Retrieve(ctx)
	if err == nil {
		log.Println("✅ Products collection exists")
		return nil
	}

	// Collection doesn't exist, create it
	log.Println("⚠️  Products collection missing, creating...")

	sortTrue := true
	defaultSortingField := "website_sequence"
	schema := &api.CollectionSchema{
		Name: "products",
		Fields: []api.Field{
			{Name: "id", Type: "string"},
			{Name: "product_id", Type: "int32"},
			{Name: "template_id", Type: "int32"},
			{Name: "categ_id", Type: "int32"},
			{Name: "name", Type: "string", Sort: &sortTrue, Infix: &sortTrue},
			{Name: "list_price", Type: "float"},
			{Name: "standard_price", Type: "float"},
			{Name: "image_url", Type: "string"},
			{Name: "msl", Type: "int32"},
			{Name: "bsl", Type: "int32"},
			{Name: "storage", Type: "string", Facet: &sortTrue},
			{Name: "uom", Type: "string"},
			{Name: "qty_available", Type: "int32"},
			{Name: "outgoing_qty", Type: "int32"},
			{Name: "product_tags", Type: "string[]", Facet: &sortTrue},
			{Name: "units_per_case", Type: "int32"},
			{Name: "barcode", Type: "string", Infix: &sortTrue},
			{Name: "rrp", Type: "float"},
			{Name: "sku", Type: "string", Infix: &sortTrue},
			{Name: "taxes", Type: "string[]"},
			{Name: "tax_percent", Type: "float"},
			{Name: "categories", Type: "int32[]", Facet: &sortTrue},
			{Name: "website_sequence", Type: "int32", Sort: &sortTrue},
			{Name: "brand", Type: "string"},
			{Name: "weight", Type: "float"},
			{Name: "case_barcode", Type: "string", Infix: &sortTrue},
		},
		DefaultSortingField: &defaultSortingField,
	}

	_, err = typesenseutil.TypesenseClient.Collections().Create(ctx, schema)
	if err != nil {
		return fmt.Errorf("failed to create products collection: %w", err)
	}

	log.Println("✅ Products collection created")
	return nil
}
