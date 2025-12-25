package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	RedisUrl      string
	Port          string
	TypesenseHost string
	TypesenseKey  string
	TypesensePort string
	OdooURL       string
}

func Load() *Config {
	// Try loading .env from multiple locations (for local dev)
	envPaths := []string{".env", "../.env", "../../.env"}
	loaded := false
	for _, path := range envPaths {
		if err := godotenv.Load(path); err == nil {
			loaded = true
			log.Printf("✅ Loaded .env from: %s", path)
			break
		}
	}
	if !loaded {
		log.Println("⚠️  No .env file found, using environment variables and defaults")
	}

	// Check if running in Docker (has /.dockerenv file)
	inDocker := false
	if _, err := os.Stat("/.dockerenv"); err == nil {
		inDocker = true
	}

	// Set defaults based on environment
	redisDefault := "redis://localhost:6379/0"
	typesenseHostDefault := "localhost"
	if inDocker {
		redisDefault = "redis://redis:6379/0"
		typesenseHostDefault = "typesense"
	}

	return &Config{
		RedisUrl:      getEnv("REDIS_URL", redisDefault),
		Port:          getEnv("PORT", "1906"),
		TypesenseHost: getEnv("TYPESENSE_HOST", typesenseHostDefault),
		TypesenseKey:  getEnv("TYPESENSE_API_KEY", "typesense-for-dhecoded-ai-dev"),
		TypesensePort: getEnv("TYPESENSE_PORT", "8108"),
		OdooURL:       getEnv("ODOO_URL", "http://localhost:8069"),
	}
}

func getEnv(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return defaultValue
}
