package worker

import (
	"log"
	"solana-txn-interra/cache"
	"solana-txn-interra/producer"
	"sync/atomic"
	"time"
)

// RedisPublisherWorker publishes rolling stats from Redis queue to Kafka
type RedisPublisherWorker struct {
	redisQueue *cache.RedisQueue
	producer   *producer.KafkaProducer
	published  int64
	errors     int64
	stopChan   chan bool
	workers    int
}

// NewRedisPublisherWorker creates a new Redis publisher worker
func NewRedisPublisherWorker(redisQueue *cache.RedisQueue, kafkaProducer *producer.KafkaProducer, workers int) *RedisPublisherWorker {
	return &RedisPublisherWorker{
		redisQueue: redisQueue,
		producer:   kafkaProducer,
		stopChan:   make(chan bool),
		workers:    workers,
	}
}

// Start starts publishing stats from Redis to Kafka
func (rpw *RedisPublisherWorker) Start() {
	log.Printf("Starting Redis publisher with %d workers", rpw.workers)

	for i := 0; i < rpw.workers; i++ {
		go rpw.worker(i)
	}
}

// worker publishes rolling stats from Redis queue to Kafka
func (rpw *RedisPublisherWorker) worker(id int) {
	log.Printf("Redis publisher worker %d started", id)

	for {
		select {
		case <-rpw.stopChan:
			log.Printf("Redis publisher worker %d stopped", id)
			return
		default:
			// Pop rolling stats from Redis queue (blocking with 1s timeout)
			stats, err := rpw.redisQueue.PopRollingStats(1 * time.Second)
			if err != nil {
				log.Printf("Publisher worker %d: Error popping stats: %v", id, err)
				atomic.AddInt64(&rpw.errors, 1)
				continue
			}

			if stats == nil {
				// Timeout, continue
				continue
			}

			// Publish to Kafka
			if err := rpw.producer.PublishRollingStats(stats); err != nil {
				log.Printf("Publisher worker %d: Error publishing to Kafka: %v", id, err)
				atomic.AddInt64(&rpw.errors, 1)
				continue
			}

			atomic.AddInt64(&rpw.published, 1)
		}
	}
}

// Stop stops the publisher
func (rpw *RedisPublisherWorker) Stop() {
	close(rpw.stopChan)
	log.Printf("Redis publisher stopped. Published: %d, Errors: %d", rpw.GetPublished(), rpw.GetErrors())
}

// GetPublished returns the number of published messages
func (rpw *RedisPublisherWorker) GetPublished() int64 {
	return atomic.LoadInt64(&rpw.published)
}

// GetErrors returns the number of errors
func (rpw *RedisPublisherWorker) GetErrors() int64 {
	return atomic.LoadInt64(&rpw.errors)
}

