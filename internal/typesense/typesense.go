package typesenseutil

import (
	"github.com/PrathameshKalekar/field-sales-go-backend/internal/config"
	"github.com/typesense/typesense-go/v4/typesense"
)

var TypesenseClient *typesense.Client

func ConnectToTypesense(config *config.Config) {
	TypesenseClient = typesense.NewClient(
		typesense.WithServer(config.TypesenseHost),
		typesense.WithAPIKey(config.TypesenseKey),
	)
}
