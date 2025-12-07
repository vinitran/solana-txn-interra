package models

import "time"

// RollingStats represents the calculated rolling statistics to be published to Kafka
type RollingStats struct {
	PairID                    string    `json:"pair_id"`
	Timestamp                 time.Time `json:"timestamp"`
	RunID                     string    `json:"run_id"`
	Last1MTxnCount            int       `json:"last_1m_txn_count"`
	Last1MTxnSellCount        int       `json:"last_1m_txn_sell_count"`
	Last1MTxnBuyCount         int       `json:"last_1m_txn_buy_count"`
	Last1MTotalVolume         float64   `json:"last_1m_total_volume"`
	Last1MTotalSellVolume     float64   `json:"last_1m_total_sell_volume"`
	Last1MTotalBuyVolume      float64   `json:"last_1m_total_buy_volume"`
	Last1MDistinctMakers      int       `json:"last_1m_distinct_makers"`
	Last1MDistinctSellMakers  int       `json:"last_1m_distinct_sell_makers"`
	Last1MDistinctBuyMakers   int       `json:"last_1m_distinct_buy_makers"`
	Last5MTxnCount            int       `json:"last_5m_txn_count"`
	Last5MTxnSellCount        int       `json:"last_5m_txn_sell_count"`
	Last5MTxnBuyCount         int       `json:"last_5m_txn_buy_count"`
	Last5MTotalVolume         float64   `json:"last_5m_total_volume"`
	Last5MTotalSellVolume     float64   `json:"last_5m_total_sell_volume"`
	Last5MTotalBuyVolume      float64   `json:"last_5m_total_buy_volume"`
	Last5MDistinctMakers      int       `json:"last_5m_distinct_makers"`
	Last5MDistinctSellMakers  int       `json:"last_5m_distinct_sell_makers"`
	Last5MDistinctBuyMakers   int       `json:"last_5m_distinct_buy_makers"`
	Last6HTxnCount            int       `json:"last_6h_txn_count"`
	Last6HTxnSellCount        int       `json:"last_6h_txn_sell_count"`
	Last6HTxnBuyCount         int       `json:"last_6h_txn_buy_count"`
	Last6HTotalVolume         float64   `json:"last_6h_total_volume"`
	Last6HTotalSellVolume     float64   `json:"last_6h_total_sell_volume"`
	Last6HTotalBuyVolume      float64   `json:"last_6h_total_buy_volume"`
	Last6HDistinctMakers      int       `json:"last_6h_distinct_makers"`
	Last6HDistinctSellMakers  int       `json:"last_6h_distinct_sell_makers"`
	Last6HDistinctBuyMakers   int       `json:"last_6h_distinct_buy_makers"`
	Last24HTxnCount           int       `json:"last_24h_txn_count"`
	Last24HTxnSellCount       int       `json:"last_24h_txn_sell_count"`
	Last24HTxnBuyCount        int       `json:"last_24h_txn_buy_count"`
	Last24HTotalVolume        float64   `json:"last_24h_total_volume"`
	Last24HTotalSellVolume    float64   `json:"last_24h_total_sell_volume"`
	Last24HTotalBuyVolume     float64   `json:"last_24h_total_buy_volume"`
	Last24HDistinctMakers     int       `json:"last_24h_distinct_makers"`
	Last24HDistinctSellMakers int       `json:"last_24h_distinct_sell_makers"`
	Last24HDistinctBuyMakers  int       `json:"last_24h_distinct_buy_makers"`
	LiquiditySOL              float64   `json:"liquidity_sol"`
	LiquidityUSD              float64   `json:"liquidity_usd"`
	PriceSOL                  float64   `json:"price_sol"`
	PriceUSD                  float64   `json:"price_usd"`
	Last1MPricePctChange      float64   `json:"last_1m_price_pct_change"`
	Last5MPricePctChange      float64   `json:"last_5m_price_pct_change"`
	Last6HPricePctChange      float64   `json:"last_6h_price_pct_change"`
	Last24HPricePctChange     float64   `json:"last_24h_price_pct_change"`
	ProcessedTimestamp        time.Time `json:"processed_timestamp"`
}
