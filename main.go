package main

import (
	"log"
	"os"
	"os/signal"
	"solana-txn-interra/cache"
	"solana-txn-interra/config"
	"solana-txn-interra/consumer"
	"solana-txn-interra/models"
	"solana-txn-interra/processor"
	"solana-txn-interra/producer"
	"syscall"
	"time"
)

// TransactionHandler implements consumer.MessageHandler
type TransactionHandler struct {
	processor       processor.Processor
	cachedProcessor *processor.CachedStatsProcessor
	producer        *producer.KafkaProducer
}

// HandleTransaction processes a transaction and publishes rolling stats
func (th *TransactionHandler) HandleTransaction(txn models.Transaction) error {
	var stats *models.RollingStats

	// Use cached processor if available, otherwise use regular processor
	if th.cachedProcessor != nil {
		stats = th.cachedProcessor.ProcessTransaction(txn)
	} else if th.processor != nil {
		stats = th.processor.ProcessTransaction(txn)
	} else {
		log.Printf("Error: no processor available")
		return nil
	}

	// Publish rolling stats to Kafka
	if err := th.producer.PublishRollingStats(stats); err != nil {
		return err
	}

	return nil
}

func main() {
	// Load configuration
	cfg := config.LoadConfig()

	// Initialize Redis cache if enabled
	var redisCache *cache.RedisCache
	var err error
	if cfg.RedisEnabled {
		redisCache, err = cache.NewRedisCache(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB, true)
		if err != nil {
			log.Printf("Warning: Failed to initialize Redis cache: %v. Continuing without Redis.", err)
			redisCache = nil
		} else {
			log.Println("Redis cache initialized successfully")
			defer redisCache.Close()
		}
	}

	// Initialize local cache
	localCache := cache.NewLocalCache(cfg.LocalCacheSize, time.Duration(cfg.LocalCacheTTL)*time.Second)
	log.Printf("Local cache initialized: size=%d, ttl=%ds", cfg.LocalCacheSize, cfg.LocalCacheTTL)

	// Initialize processor (use cached processor if Redis is enabled, otherwise use regular)
	var handler *TransactionHandler
	if cfg.RedisEnabled && redisCache != nil {
		cachedProcessor := processor.NewCachedStatsProcessor(redisCache, localCache)
		defer cachedProcessor.Close()
		handler = &TransactionHandler{
			cachedProcessor: cachedProcessor,
			producer:        nil, // Will be set below
		}
		log.Println("Using cached processor with Redis")
	} else {
		statsProcessor := processor.NewStatsProcessor()
		handler = &TransactionHandler{
			processor: statsProcessor,
			producer:  nil, // Will be set below
		}
		log.Println("Using regular processor without Redis")
	}

	// Initialize producer
	kafkaProducer, err := producer.NewKafkaProducer(cfg)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer kafkaProducer.Close()
	handler.producer = kafkaProducer

	// Initialize consumer
	kafkaConsumer, err := consumer.NewKafkaConsumer(cfg, handler)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer kafkaConsumer.Close()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start consumer in a goroutine
	go func() {
		if err := kafkaConsumer.Start(); err != nil {
			log.Fatalf("Error from consumer: %v", err)
		}
	}()

	log.Println("Transaction processor started. Press Ctrl+C to stop.")

	// Wait for interrupt signal
	<-sigChan
	log.Println("Shutting down...")
}
