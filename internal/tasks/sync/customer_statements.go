package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/PrathameshKalekar/field-sales-go-backend/internal/config"
	"github.com/PrathameshKalekar/field-sales-go-backend/internal/odoo"
	redisutil "github.com/PrathameshKalekar/field-sales-go-backend/internal/redis"
	"github.com/hibiken/asynq"
)

func HandleSyncCustomerStatementsTask(ctx context.Context, t *asynq.Task) error {
	defer MarkCoreTaskCompletion(ctx)
	log.Println("🔄 Syncing customer statements (ledger-based, 6 months)...")

	// Calculate period start (6 months ago)
	today := time.Now()
	periodStart := today.AddDate(0, -6, 0)
	periodStartStr := periodStart.Format("2006-01-02")

	log.Printf("📅 Statement period starts from: %s", periodStartStr)

	// 1. Fetch ALL posted receivable ledger lines
	ledgerLines, err := odooSearchRead(
		"account.move.line",
		[]any{
			[]any{"partner_id", "!=", false},
			[]any{"move_id.state", "=", "posted"},
			[]any{"account_id.account_type", "=", "asset_receivable"},
		},
		[]string{"id", "date", "partner_id", "debit", "credit", "move_id"},
		5000,
	)
	if err != nil {
		log.Printf("❌ Failed to fetch ledger lines: %v", err)
		return err
	}

	log.Printf("✅ Retrieved %d ledger lines for customer statements", len(ledgerLines))

	if len(ledgerLines) == 0 {
		log.Println("✅ No ledger lines found")
		return nil
	}

	// 2. Sort ledger lines (Odoo order: date, move_id, id)
	sort.Slice(ledgerLines, func(i, j int) bool {
		dateI := getString(ledgerLines[i]["date"])
		dateJ := getString(ledgerLines[j]["date"])
		if dateI != dateJ {
			return dateI < dateJ
		}

		moveIDI := getMoveID(ledgerLines[i]["move_id"])
		moveIDJ := getMoveID(ledgerLines[j]["move_id"])
		if moveIDI != moveIDJ {
			return moveIDI < moveIDJ
		}

		idI := getInt(ledgerLines[i]["id"])
		idJ := getInt(ledgerLines[j]["id"])
		return idI < idJ
	})

	// 3. Fetch related moves
	moveIDs := make(map[int]bool)
	for _, line := range ledgerLines {
		if moveID := getMoveID(line["move_id"]); moveID > 0 {
			moveIDs[moveID] = true
		}
	}

	moveIDList := make([]int, 0, len(moveIDs))
	for id := range moveIDs {
		moveIDList = append(moveIDList, id)
	}

	moves, err := fetchMoves(moveIDList)
	if err != nil {
		log.Printf("❌ Failed to fetch moves: %v", err)
		return err
	}

	// 4. Group ledger lines per customer
	partnerGroups := make(map[int][]map[string]any)
	for _, line := range ledgerLines {
		partnerID := getPartnerID(line["partner_id"])
		if partnerID > 0 {
			partnerGroups[partnerID] = append(partnerGroups[partnerID], line)
		}
	}

	// 5. Build statement per customer (6 months logic)
	processedCount := 0
	for partnerID, lines := range partnerGroups {
		if len(lines) == 0 {
			continue
		}

		partnerName := getPartnerName(lines[0]["partner_id"])

		openingBalance := 0.0
		runningBalance := 0.0
		rows := []map[string]any{}

		for _, line := range lines {
			lineDate := getString(line["date"])
			debit := getFloat(line["debit"])
			credit := getFloat(line["credit"])
			delta := debit - credit

			// BEFORE period → opening balance
			if lineDate < periodStartStr {
				openingBalance += delta
				continue
			}

			// INSIDE period → statement row
			runningBalance += delta

			moveID := getMoveID(line["move_id"])
			move := moves[moveID]

			moveType := getString(move["move_type"])
			invoiceID := getInt(move["id"])

			var pdfURL any
			if moveType == "out_invoice" || moveType == "out_refund" {
				pdfURL = fmt.Sprintf("%s/report/pdf/account.report_invoice/%d", config.ConfigGlobal.OdooURL, invoiceID)
			} else {
				pdfURL = nil
			}

			journal := ""
			if journalID, ok := move["journal_id"].([]any); ok && len(journalID) > 1 {
				journal = getString(journalID[1])
			}

			rows = append(rows, map[string]any{
				"partner_id":    partnerID,
				"partner_name":  partnerName,
				"journal":       journal,
				"invoice_id":    invoiceID,
				"invoice_name":  getString(move["name"]),
				"invoice_date":  lineDate,
				"pdf_url":       pdfURL,
				"debit":         roundFloat(debit, 2),
				"credit":        roundFloat(credit, 2),
				"running_balance": roundFloat(openingBalance+runningBalance, 2),
			})
		}

		// Skip customers with no activity in last 6 months
		if len(rows) == 0 {
			continue
		}

		customerStatement := map[string]any{
			"partner_id":      partnerID,
			"partner_name":    partnerName,
			"opening_balance": roundFloat(openingBalance, 2),
			"closing_balance": roundFloat(openingBalance+runningBalance, 2),
			"entries":         rows,
		}

		// 6. Store per partner in Redis
		redisKey := fmt.Sprintf("dashboard:customer_statement:%d", partnerID)
		statementJSON, err := json.Marshal(customerStatement)
		if err != nil {
			log.Printf("⚠️  Failed to marshal statement for partner %d: %v", partnerID, err)
			continue
		}

		if err := redisutil.RedisClient.Set(ctx, redisKey, statementJSON, 0).Err(); err != nil {
			log.Printf("⚠️  Failed to save statement for partner %d: %v", partnerID, err)
			continue
		}

		processedCount++
	}

	log.Printf("✅ Customer statements synced successfully - %d customers processed", processedCount)
	return nil
}

// odooSearchRead fetches data from Odoo in batches
func odooSearchRead(model string, domain []any, fields []string, batchSize int) ([]map[string]any, error) {
	offset := 0
	results := []map[string]any{}

	for {
		payload := map[string]any{
			"jsonrpc": "2.0",
			"method":  "call",
			"params": map[string]any{
				"model":  model,
				"method": "search_read",
				"args":   []any{domain},
				"kwargs": map[string]any{
					"fields": fields,
					"limit":  batchSize,
					"offset": offset,
				},
			},
			"id": 2,
		}

		resp, err := odoo.OdooManager.NewRequest("POST", "web/dataset/call_kw", payload)
		if err != nil {
			return nil, err
		}

		var rpcResp struct {
			Result []map[string]any `json:"result"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()

		if len(rpcResp.Result) == 0 {
			break
		}

		results = append(results, rpcResp.Result...)
		offset += batchSize
	}

	return results, nil
}

// fetchMoves fetches account.move records by IDs
func fetchMoves(moveIDs []int) (map[int]map[string]any, error) {
	if len(moveIDs) == 0 {
		return make(map[int]map[string]any), nil
	}

	moves, err := odooSearchRead(
		"account.move",
		[]any{[]any{"id", "in", moveIDs}},
		[]string{"id", "name", "move_type", "journal_id"},
		5000,
	)
	if err != nil {
		return nil, err
	}

	moveMap := make(map[int]map[string]any)
	for _, move := range moves {
		id := getInt(move["id"])
		moveMap[id] = move
	}

	return moveMap, nil
}

// Helper functions
func getInt(v any) int {
	if v == nil {
		return 0
	}
	if f, ok := v.(float64); ok {
		return int(f)
	}
	return 0
}

func getFloat(v any) float64 {
	if v == nil {
		return 0.0
	}
	if f, ok := v.(float64); ok {
		return f
	}
	return 0.0
}

func getString(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func getPartnerID(v any) int {
	if list, ok := v.([]any); ok && len(list) > 0 {
		return getInt(list[0])
	}
	return 0
}

func getPartnerName(v any) string {
	if list, ok := v.([]any); ok && len(list) > 1 {
		return getString(list[1])
	}
	return ""
}

func getMoveID(v any) int {
	if list, ok := v.([]any); ok && len(list) > 0 {
		return getInt(list[0])
	}
	return 0
}

func roundFloat(val float64, precision int) float64 {
	multiplier := 1.0
	for i := 0; i < precision; i++ {
		multiplier *= 10
	}
	return float64(int(val*multiplier+0.5)) / multiplier
}
