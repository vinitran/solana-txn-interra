package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"solana-txn-interra/models"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache handles Redis operations for caching transaction data
type RedisCache struct {
	client  *redis.Client
	ctx     context.Context
	enabled bool
}

// NewRedisCache creates a new Redis cache client
func NewRedisCache(addr, password string, db int, enabled bool) (*RedisCache, error) {
	if !enabled {
		return &RedisCache{enabled: false}, nil
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		PoolSize:     100, // Connection pool size for high throughput
		MinIdleConns: 10,
		MaxRetries:   3,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	ctx := context.Background()
	// Test connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisCache{
		client:  rdb,
		ctx:     ctx,
		enabled: true,
	}, nil
}

// AddTransaction adds a transaction to Redis sorted set for a specific pair and timeframe
// Key format: txns:{pair_id}:{timeframe}
// Score: timestamp as UnixNano
func (rc *RedisCache) AddTransaction(pairID, timeframe string, txn models.Transaction) error {
	if !rc.enabled {
		return nil
	}

	key := fmt.Sprintf("txns:%s:%s", pairID, timeframe)
	score := float64(txn.Timestamp.UnixNano())

	// Serialize transaction (only essential fields to save memory)
	txnData := map[string]interface{}{
		"type":       txn.Type,
		"amount_usd": txn.AmountUSD,
		"price_usd":  txn.PriceUSD,
		"maker":      txn.Maker,
		"timestamp":  txn.Timestamp.UnixNano(),
	}

	jsonData, err := json.Marshal(txnData)
	if err != nil {
		return fmt.Errorf("failed to marshal transaction: %w", err)
	}

	// Add to sorted set
	if err := rc.client.ZAdd(rc.ctx, key, redis.Z{
		Score:  score,
		Member: jsonData,
	}).Err(); err != nil {
		return fmt.Errorf("failed to add transaction to Redis: %w", err)
	}

	// Set expiration for the key (timeframe duration + buffer)
	var ttl time.Duration
	switch timeframe {
	case "1m":
		ttl = 2 * time.Minute
	case "5m":
		ttl = 6 * time.Minute
	case "6h":
		ttl = 7 * time.Hour
	case "24h":
		ttl = 25 * time.Hour
	default:
		ttl = 25 * time.Hour
	}
	rc.client.Expire(rc.ctx, key, ttl)

	return nil
}

// RemoveOldTransactions removes transactions older than cutoffTime from Redis
func (rc *RedisCache) RemoveOldTransactions(pairID, timeframe string, cutoffTime time.Time) error {
	if !rc.enabled {
		return nil
	}

	key := fmt.Sprintf("txns:%s:%s", pairID, timeframe)
	cutoffScore := float64(cutoffTime.UnixNano())

	// Remove transactions with score < cutoffScore
	if err := rc.client.ZRemRangeByScore(rc.ctx, key, "0", fmt.Sprintf("%.0f", cutoffScore-1)).Err(); err != nil {
		return fmt.Errorf("failed to remove old transactions: %w", err)
	}

	return nil
}

// GetTransactionsInRange retrieves transactions within a time range from Redis
func (rc *RedisCache) GetTransactionsInRange(pairID, timeframe string, startTime, endTime time.Time) ([]models.Transaction, error) {
	if !rc.enabled {
		return nil, nil
	}

	key := fmt.Sprintf("txns:%s:%s", pairID, timeframe)
	startScore := float64(startTime.UnixNano())
	endScore := float64(endTime.UnixNano())

	// Get transactions in range
	results, err := rc.client.ZRangeByScore(rc.ctx, key, &redis.ZRangeBy{
		Min: fmt.Sprintf("%.0f", startScore),
		Max: fmt.Sprintf("%.0f", endScore),
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to get transactions from Redis: %w", err)
	}

	transactions := make([]models.Transaction, 0, len(results))
	for _, result := range results {
		var txnData map[string]interface{}
		if err := json.Unmarshal([]byte(result), &txnData); err != nil {
			continue
		}

		// Reconstruct transaction (minimal fields)
		txn := models.Transaction{
			Type:      getString(txnData, "type"),
			AmountUSD: getFloat64(txnData, "amount_usd"),
			PriceUSD:  getFloat64(txnData, "price_usd"),
			Maker:     getString(txnData, "maker"),
		}
		if ts, ok := txnData["timestamp"].(float64); ok {
			txn.Timestamp = time.Unix(0, int64(ts))
		}

		transactions = append(transactions, txn)
	}

	return transactions, nil
}

// GetTransactionCount returns the count of transactions in a timeframe
func (rc *RedisCache) GetTransactionCount(pairID, timeframe string) (int64, error) {
	if !rc.enabled {
		return 0, nil
	}

	key := fmt.Sprintf("txns:%s:%s", pairID, timeframe)
	count, err := rc.client.ZCard(rc.ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get transaction count: %w", err)
	}

	return count, nil
}

// SetAggregatedStats caches aggregated statistics for a pair and timeframe
func (rc *RedisCache) SetAggregatedStats(pairID, timeframe string, stats *AggregatedStats, ttl time.Duration) error {
	if !rc.enabled {
		return nil
	}

	key := fmt.Sprintf("stats:%s:%s", pairID, timeframe)
	data, err := json.Marshal(stats)
	if err != nil {
		return fmt.Errorf("failed to marshal stats: %w", err)
	}

	if err := rc.client.Set(rc.ctx, key, data, ttl).Err(); err != nil {
		return fmt.Errorf("failed to set aggregated stats: %w", err)
	}

	return nil
}

// GetAggregatedStats retrieves cached aggregated statistics
func (rc *RedisCache) GetAggregatedStats(pairID, timeframe string) (*AggregatedStats, error) {
	if !rc.enabled {
		return nil, nil
	}

	key := fmt.Sprintf("stats:%s:%s", pairID, timeframe)
	data, err := rc.client.Get(rc.ctx, key).Result()
	if err == redis.Nil {
		return nil, nil // Cache miss
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get aggregated stats: %w", err)
	}

	var stats AggregatedStats
	if err := json.Unmarshal([]byte(data), &stats); err != nil {
		return nil, fmt.Errorf("failed to unmarshal stats: %w", err)
	}

	return &stats, nil
}

// Close closes the Redis connection
func (rc *RedisCache) Close() error {
	if !rc.enabled {
		return nil
	}
	return rc.client.Close()
}

// AggregatedStats holds pre-calculated statistics
type AggregatedStats struct {
	TotalVolume float64   `json:"total_volume"`
	BuyVolume   float64   `json:"buy_volume"`
	SellVolume  float64   `json:"sell_volume"`
	BuyCount    int       `json:"buy_count"`
	SellCount   int       `json:"sell_count"`
	BuyMakers   []string  `json:"buy_makers"`
	SellMakers  []string  `json:"sell_makers"`
	OpenPrice   float64   `json:"open_price"`
	LastUpdated time.Time `json:"last_updated"`
}

// Helper functions
func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getFloat64(m map[string]interface{}, key string) float64 {
	if v, ok := m[key].(float64); ok {
		return v
	}
	return 0
}
