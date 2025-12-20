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
}

func Load() *Config {
	if err := godotenv.Load("../.env"); err != nil {
		log.Println("No .env file found")
	}

	return &Config{
		RedisUrl:      getEnv("REDIS_URL"),
		Port:          getEnv("PORT"),
		TypesenseHost: getEnv("TYPESENSE_HOST"),
		TypesenseKey:  getEnv("TYPESENSE_API_KEY"),
		TypesensePort: getEnv("TYPESENSE_PORT"),
	}
}

func getEnv(key string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return ""
}
