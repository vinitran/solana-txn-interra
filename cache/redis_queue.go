package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"solana-txn-interra/models"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisQueue handles Redis queue operations for transactions and stats
type RedisQueue struct {
	client  *redis.Client
	ctx     context.Context
	enabled bool
}

// NewRedisQueue creates a new Redis queue client
func NewRedisQueue(addr, password string, db int, enabled bool) (*RedisQueue, error) {
	if !enabled {
		return &RedisQueue{enabled: false}, nil
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		PoolSize:     100,
		MinIdleConns: 10,
		MaxRetries:   3,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisQueue{
		client:  rdb,
		ctx:     ctx,
		enabled: true,
	}, nil
}

// PushTransaction pushes a transaction to Redis queue
func (rq *RedisQueue) PushTransaction(txn models.Transaction) error {
	if !rq.enabled {
		return fmt.Errorf("Redis queue is not enabled")
	}

	data, err := json.Marshal(txn)
	if err != nil {
		return fmt.Errorf("failed to marshal transaction: %w", err)
	}

	// Push to transaction queue
	if err := rq.client.LPush(rq.ctx, "queue:transactions", data).Err(); err != nil {
		return fmt.Errorf("failed to push transaction to queue: %w", err)
	}

	return nil
}

// PopTransaction pops a transaction from Redis queue (blocking)
func (rq *RedisQueue) PopTransaction(timeout time.Duration) (*models.Transaction, error) {
	if !rq.enabled {
		return nil, fmt.Errorf("Redis queue is not enabled")
	}

	// Blocking pop from queue
	result, err := rq.client.BRPop(rq.ctx, timeout, "queue:transactions").Result()
	if err == redis.Nil {
		return nil, nil // Timeout, no message
	}
	if err != nil {
		return nil, fmt.Errorf("failed to pop transaction from queue: %w", err)
	}

	if len(result) < 2 {
		return nil, fmt.Errorf("invalid result from BRPop")
	}

	var txn models.Transaction
	if err := json.Unmarshal([]byte(result[1]), &txn); err != nil {
		return nil, fmt.Errorf("failed to unmarshal transaction: %w", err)
	}

	return &txn, nil
}

// PushRollingStats pushes rolling stats to Redis output queue
func (rq *RedisQueue) PushRollingStats(stats *models.RollingStats) error {
	if !rq.enabled {
		return fmt.Errorf("Redis queue is not enabled")
	}

	data, err := json.Marshal(stats)
	if err != nil {
		return fmt.Errorf("failed to marshal rolling stats: %w", err)
	}

	// Push to stats queue
	if err := rq.client.LPush(rq.ctx, "queue:rolling-stats", data).Err(); err != nil {
		return fmt.Errorf("failed to push rolling stats to queue: %w", err)
	}

	return nil
}

// PopRollingStats pops rolling stats from Redis queue (blocking)
func (rq *RedisQueue) PopRollingStats(timeout time.Duration) (*models.RollingStats, error) {
	if !rq.enabled {
		return nil, fmt.Errorf("Redis queue is not enabled")
	}

	// Blocking pop from queue
	result, err := rq.client.BRPop(rq.ctx, timeout, "queue:rolling-stats").Result()
	if err == redis.Nil {
		return nil, nil // Timeout, no message
	}
	if err != nil {
		return nil, fmt.Errorf("failed to pop rolling stats from queue: %w", err)
	}

	if len(result) < 2 {
		return nil, fmt.Errorf("invalid result from BRPop")
	}

	var stats models.RollingStats
	if err := json.Unmarshal([]byte(result[1]), &stats); err != nil {
		return nil, fmt.Errorf("failed to unmarshal rolling stats: %w", err)
	}

	return &stats, nil
}

// GetQueueLength returns the length of a queue
func (rq *RedisQueue) GetQueueLength(queueName string) (int64, error) {
	if !rq.enabled {
		return 0, nil
	}

	length, err := rq.client.LLen(rq.ctx, queueName).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get queue length: %w", err)
	}

	return length, nil
}

// Close closes the Redis connection
func (rq *RedisQueue) Close() error {
	if !rq.enabled {
		return nil
	}
	return rq.client.Close()
}

