package cache

import (
	"solana-txn-interra/models"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
)

// LocalCache provides fast in-memory caching for hot data
type LocalCache struct {
	// LRU cache for aggregated stats: pair_id:timeframe -> AggregatedStats
	statsCache *expirable.LRU[string, *AggregatedStats]
	// LRU cache for open prices: pair_id:timeframe -> float64
	openPriceCache *expirable.LRU[string, float64]
	// Mutex for thread-safe operations
	mu sync.RWMutex
}

// NewLocalCache creates a new local cache with specified size and TTL
func NewLocalCache(size int, ttl time.Duration) *LocalCache {
	return &LocalCache{
		statsCache: expirable.NewLRU[string, *AggregatedStats](size, nil, ttl),
		openPriceCache: expirable.NewLRU[string, float64](size, nil, ttl),
	}
}

// GetStats retrieves cached aggregated stats
func (lc *LocalCache) GetStats(pairID, timeframe string) (*AggregatedStats, bool) {
	lc.mu.RLock()
	defer lc.mu.RUnlock()

	key := lc.makeKey(pairID, timeframe)
	stats, ok := lc.statsCache.Get(key)
	return stats, ok
}

// SetStats caches aggregated stats
func (lc *LocalCache) SetStats(pairID, timeframe string, stats *AggregatedStats) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	key := lc.makeKey(pairID, timeframe)
	lc.statsCache.Add(key, stats)
}

// GetOpenPrice retrieves cached open price
func (lc *LocalCache) GetOpenPrice(pairID, timeframe string) (float64, bool) {
	lc.mu.RLock()
	defer lc.mu.RUnlock()

	key := lc.makeKey(pairID, timeframe)
	price, ok := lc.openPriceCache.Get(key)
	return price, ok
}

// SetOpenPrice caches open price
func (lc *LocalCache) SetOpenPrice(pairID, timeframe string, price float64) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	key := lc.makeKey(pairID, timeframe)
	lc.openPriceCache.Add(key, price)
}

// InvalidateStats removes cached stats for a pair and timeframe
func (lc *LocalCache) InvalidateStats(pairID, timeframe string) {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	key := lc.makeKey(pairID, timeframe)
	lc.statsCache.Remove(key)
	lc.openPriceCache.Remove(key)
}

// makeKey creates a cache key from pairID and timeframe
func (lc *LocalCache) makeKey(pairID, timeframe string) string {
	return pairID + ":" + timeframe
}

// TransactionBuffer holds recent transactions in memory for fast access
type TransactionBuffer struct {
	transactions []models.Transaction
	mu           sync.RWMutex
	maxSize      int
}

// NewTransactionBuffer creates a new transaction buffer
func NewTransactionBuffer(maxSize int) *TransactionBuffer {
	return &TransactionBuffer{
		transactions: make([]models.Transaction, 0, maxSize),
		maxSize:      maxSize,
	}
}

// Add adds a transaction to the buffer
func (tb *TransactionBuffer) Add(txn models.Transaction) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.transactions = append(tb.transactions, txn)
	if len(tb.transactions) > tb.maxSize {
		// Remove oldest transaction
		tb.transactions = tb.transactions[1:]
	}
}

// GetRecent returns transactions after cutoffTime
func (tb *TransactionBuffer) GetRecent(cutoffTime time.Time) []models.Transaction {
	tb.mu.RLock()
	defer tb.mu.RUnlock()

	result := make([]models.Transaction, 0)
	for _, txn := range tb.transactions {
		if txn.Timestamp.After(cutoffTime) || txn.Timestamp.Equal(cutoffTime) {
			result = append(result, txn)
		}
	}
	return result
}

// Clear removes all transactions
func (tb *TransactionBuffer) Clear() {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.transactions = tb.transactions[:0]
}

