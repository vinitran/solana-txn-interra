package main

import (
	"log"
	"os"
	"os/signal"
	"solana-txn-interra/cache"
	"solana-txn-interra/config"
	"solana-txn-interra/consumer"
	"solana-txn-interra/models"
	"solana-txn-interra/producer"
	"solana-txn-interra/worker"
	"syscall"
	"time"
)

// TransactionHandler implements consumer.MessageHandler - pushes to Redis queue
type TransactionHandler struct {
	redisQueue *cache.RedisQueue
}

// HandleTransaction pushes transaction to Redis queue
func (th *TransactionHandler) HandleTransaction(txn models.Transaction) error {
	// Push transaction to Redis queue
	return th.redisQueue.PushTransaction(txn)
}

func main() {
	// Load configuration
	cfg := config.LoadConfig()

	if !cfg.RedisEnabled {
		log.Fatalf("Redis must be enabled for Redis-first architecture. Set REDIS_ENABLED=true")
	}

	// Initialize Redis queue
	redisQueue, err := cache.NewRedisQueue(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB, true)
	if err != nil {
		log.Fatalf("Failed to initialize Redis queue: %v", err)
	}
	defer redisQueue.Close()
	log.Println("Redis queue initialized successfully")

	// Initialize Redis cache
	redisCache, err := cache.NewRedisCache(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB, true)
	if err != nil {
		log.Fatalf("Failed to initialize Redis cache: %v", err)
	}
	defer redisCache.Close()
	log.Println("Redis cache initialized successfully")

	// Initialize local cache
	localCache := cache.NewLocalCache(cfg.LocalCacheSize, time.Duration(cfg.LocalCacheTTL)*time.Second)
	log.Printf("Local cache initialized: size=%d, ttl=%ds", cfg.LocalCacheSize, cfg.LocalCacheTTL)

	// Initialize processor worker (reads from Redis, processes, writes to Redis)
	processorWorker := worker.NewRedisProcessorWorker(redisQueue, redisCache, localCache, cfg.WorkerPoolSize)
	processorWorker.Start()
	defer processorWorker.Stop()

	// Initialize producer
	kafkaProducer, err := producer.NewKafkaProducer(cfg)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer kafkaProducer.Close()

	// Initialize publisher worker (reads from Redis, publishes to Kafka)
	publisherWorker := worker.NewRedisPublisherWorker(redisQueue, kafkaProducer, cfg.WorkerPoolSize/2) // Half workers for publishing
	publisherWorker.Start()
	defer publisherWorker.Stop()

	// Create handler (only pushes to Redis)
	handler := &TransactionHandler{
		redisQueue: redisQueue,
	}

	// Start metrics reporting goroutine
	go reportMetricsRedis(processorWorker, publisherWorker, kafkaProducer, redisQueue)

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

// reportMetricsRedis periodically reports processing metrics for Redis-first architecture
func reportMetricsRedis(processorWorker *worker.RedisProcessorWorker, publisherWorker *worker.RedisPublisherWorker, producer *producer.KafkaProducer, redisQueue *cache.RedisQueue) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		processed := processorWorker.GetProcessed()
		procErrors := processorWorker.GetErrors()
		published := publisherWorker.GetPublished()
		pubErrors := publisherWorker.GetErrors()
		successes, prodErrors, dropped := producer.GetStats()

		// Get queue lengths
		txnQueueLen, _ := redisQueue.GetQueueLength("queue:transactions")
		statsQueueLen, _ := redisQueue.GetQueueLength("queue:rolling-stats")

		log.Printf("Metrics - Processed: %d, Proc Errors: %d, Published: %d, Pub Errors: %d, Kafka Published: %d, Kafka Errors: %d, Dropped: %d",
			processed, procErrors, published, pubErrors, successes, prodErrors, dropped)
		log.Printf("Queue Lengths - Transactions: %d, Rolling Stats: %d", txnQueueLen, statsQueueLen)

		if dropped > 0 {
			log.Printf("WARNING: %d messages dropped due to producer queue full. Consider increasing PRODUCER_BUFFER_SIZE", dropped)
		}

		if txnQueueLen > 10000 {
			log.Printf("WARNING: Transaction queue is backing up (%d messages). Consider increasing processor workers.", txnQueueLen)
		}

		if statsQueueLen > 10000 {
			log.Printf("WARNING: Stats queue is backing up (%d messages). Consider increasing publisher workers.", statsQueueLen)
		}
	}
}
