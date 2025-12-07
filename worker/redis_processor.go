package worker

import (
	"log"
	"solana-txn-interra/cache"
	"solana-txn-interra/processor"
	"sync/atomic"
	"time"
)

// RedisProcessorWorker processes transactions from Redis queue
type RedisProcessorWorker struct {
	redisQueue     *cache.RedisQueue
	redisCache     *cache.RedisCache
	localCache     *cache.LocalCache
	processor      *processor.CachedStatsProcessor
	processed      int64
	errors         int64
	stopChan       chan bool
	workers        int
}

// NewRedisProcessorWorker creates a new Redis processor worker
func NewRedisProcessorWorker(redisQueue *cache.RedisQueue, redisCache *cache.RedisCache, localCache *cache.LocalCache, workers int) *RedisProcessorWorker {
	proc := processor.NewCachedStatsProcessor(redisCache, localCache)

	return &RedisProcessorWorker{
		redisQueue: redisQueue,
		redisCache: redisCache,
		localCache: localCache,
		processor:  proc,
		stopChan:   make(chan bool),
		workers:    workers,
	}
}

// Start starts processing transactions from Redis
func (rpw *RedisProcessorWorker) Start() {
	log.Printf("Starting Redis processor with %d workers", rpw.workers)

	for i := 0; i < rpw.workers; i++ {
		go rpw.worker(i)
	}
}

// worker processes transactions from Redis queue
func (rpw *RedisProcessorWorker) worker(id int) {
	log.Printf("Redis processor worker %d started", id)

	for {
		select {
		case <-rpw.stopChan:
			log.Printf("Redis processor worker %d stopped", id)
			return
		default:
			// Pop transaction from Redis queue (blocking with 1s timeout)
			txn, err := rpw.redisQueue.PopTransaction(1 * time.Second)
			if err != nil {
				log.Printf("Worker %d: Error popping transaction: %v", id, err)
				atomic.AddInt64(&rpw.errors, 1)
				continue
			}

			if txn == nil {
				// Timeout, continue
				continue
			}

			// Process transaction
			stats := rpw.processor.ProcessTransaction(*txn)

			if stats != nil {
				// Push rolling stats to output queue
				if err := rpw.redisQueue.PushRollingStats(stats); err != nil {
					log.Printf("Worker %d: Error pushing rolling stats: %v", id, err)
					atomic.AddInt64(&rpw.errors, 1)
					continue
				}
			}

			atomic.AddInt64(&rpw.processed, 1)
		}
	}
}

// Stop stops the processor
func (rpw *RedisProcessorWorker) Stop() {
	close(rpw.stopChan)
	rpw.processor.Close()
	log.Printf("Redis processor stopped. Processed: %d, Errors: %d", rpw.GetProcessed(), rpw.GetErrors())
}

// GetProcessed returns the number of processed transactions
func (rpw *RedisProcessorWorker) GetProcessed() int64 {
	return atomic.LoadInt64(&rpw.processed)
}

// GetErrors returns the number of errors
func (rpw *RedisProcessorWorker) GetErrors() int64 {
	return atomic.LoadInt64(&rpw.errors)
}

