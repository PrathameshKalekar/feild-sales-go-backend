package typesenseutil

import (
	"context"
	"log"
	"time"

	"github.com/PrathameshKalekar/field-sales-go-backend/internal/config"
	"github.com/typesense/typesense-go/v4/typesense"
)

var TypesenseClient *typesense.Client

func ConnectToTypesense(config *config.Config) {
	TypesenseClient = typesense.NewClient(
		typesense.WithServer(config.TypesenseHost),
		typesense.WithAPIKey(config.TypesenseKey),
	)

	_, err := TypesenseClient.Health(context.Background(), 30*time.Second)
	if err != nil {
		log.Fatalf("❌ Failed to connect to Typesense: %v", err)
	}

	log.Printf("✅ Typesense connected successfully to %s\n", config.TypesenseHost)
}
