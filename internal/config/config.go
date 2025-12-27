package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

var ConfigGlobal *Config

type Config struct {
	RedisUrl      string
	Port          string
	TypesenseHost string
	TypesenseKey  string
	TypesensePort string
	OdooURL       string
	OdooDB        string
	OdooUsername  string
	OdooPassword  string
}

func Load() {

	if err := godotenv.Load(".env"); err == nil {
		log.Println("✅ Loaded .env")
	}

	ConfigGlobal = &Config{
		RedisUrl:      getEnv("REDIS_URL"),
		Port:          getEnv("PORT"),
		TypesenseHost: getEnv("TYPESENSE_HOST"),
		TypesenseKey:  getEnv("TYPESENSE_API_KEY"),
		TypesensePort: getEnv("TYPESENSE_PORT"),
		OdooURL:       getEnv("ODOO_URL"),
		OdooDB:        getEnv("ODOO_DB"),
		OdooUsername:  getEnv("ODOO_USERNAME"),
		OdooPassword:  getEnv("ODOO_PASSWORD"),
	}

}

func getEnv(key string) string {
	return os.Getenv(key)
}
