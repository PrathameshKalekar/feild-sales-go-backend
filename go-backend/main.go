package main

import (
	"github.com/PrathameshKalekar/field-sales-go-backend/internal/config"
	redisUtil "github.com/PrathameshKalekar/field-sales-go-backend/internal/redis"
	typesenseUtil "github.com/PrathameshKalekar/field-sales-go-backend/internal/typesense"
	"github.com/gin-gonic/gin"
)

func main() {
	gin.SetMode(gin.ReleaseMode)
	cfg := config.Load()
	redisUtil.ConnectToRedis(cfg)
	typesenseUtil.ConnectToTypesense(cfg)
	
}
