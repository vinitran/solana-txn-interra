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
	"solana-txn-interra/worker"
	"syscall"
	"time"
)

// TransactionHandler implements consumer.MessageHandler with worker pool
type TransactionHandler struct {
	processor       processor.Processor
	cachedProcessor *processor.CachedStatsProcessor
	producer        *producer.KafkaProducer
	workerPool      *worker.Pool
}

// HandleTransaction submits transaction to worker pool for async processing
func (th *TransactionHandler) HandleTransaction(txn models.Transaction) error {
	// Submit to worker pool for async processing
	th.workerPool.Submit(worker.Job{
		Transaction: txn,
		ProcessFunc: th.processTransaction,
		PublishFunc: th.publishStats,
	})
	return nil
}

// processTransaction processes a transaction and returns rolling stats
func (th *TransactionHandler) processTransaction(txn models.Transaction) (*models.RollingStats, error) {
	var stats *models.RollingStats

	// Use cached processor if available, otherwise use regular processor
	if th.cachedProcessor != nil {
		stats = th.cachedProcessor.ProcessTransaction(txn)
	} else if th.processor != nil {
		stats = th.processor.ProcessTransaction(txn)
	} else {
		return nil, nil
	}

	return stats, nil
}

// publishStats publishes rolling stats to Kafka (realtime mode)
func (th *TransactionHandler) publishStats(stats *models.RollingStats) error {
	if stats == nil {
		return nil
	}
	// Always use async publish for realtime (non-blocking)
	return th.producer.PublishRollingStats(stats)
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

	// Initialize worker pool
	workerPool := worker.NewPool(cfg.WorkerPoolSize, cfg.WorkerPoolSize*2) // Queue size = 2x workers
	workerPool.Start()
	defer workerPool.Stop()

	// Initialize processor (use cached processor if Redis is enabled, otherwise use regular)
	var proc processor.Processor
	var cachedProc *processor.CachedStatsProcessor
	if cfg.RedisEnabled && redisCache != nil {
		cachedProc = processor.NewCachedStatsProcessor(redisCache, localCache)
		defer cachedProc.Close()
		log.Println("Using cached processor with Redis")
	} else {
		proc = processor.NewStatsProcessor()
		log.Println("Using regular processor without Redis")
	}

	// Initialize producer
	kafkaProducer, err := producer.NewKafkaProducer(cfg)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer kafkaProducer.Close()

	// Create handler with worker pool
	handler := &TransactionHandler{
		processor:       proc,
		cachedProcessor: cachedProc,
		producer:        kafkaProducer,
		workerPool:      workerPool,
	}

	// Start metrics reporting goroutine
	go reportMetrics(workerPool, kafkaProducer)

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
	cfg.PrintConfig()

	// Wait for interrupt signal
	<-sigChan
	log.Println("Shutting down...")
}

// reportMetrics periodically reports processing metrics
func reportMetrics(workerPool *worker.Pool, producer *producer.KafkaProducer) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		processed := workerPool.GetProcessed()
		errors := workerPool.GetErrors()
		tps := workerPool.GetTPS()
		successes, prodErrors := producer.GetStats()

		log.Printf("Metrics - Processed: %d, Errors: %d, TPS: %.2f, Published: %d, Publish Errors: %d",
			processed, errors, tps, successes, prodErrors)
	}
}
