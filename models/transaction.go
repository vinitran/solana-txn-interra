package models

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// Transaction represents a transaction from Kafka topic transaction-v3
type Transaction struct {
	Timestamp       time.Time `json:"timestamp"`
	Type            string    `json:"type"`
	AmountSOL       float64   `json:"amount_sol"`
	AmountUSD       float64   `json:"amount_usd"`
	AmountToken     float64   `json:"amount_token"`
	PriceSOL        float64   `json:"price_sol"`
	PriceUSD        float64   `json:"price_usd"`
	Maker           string    `json:"maker"`
	Txn             string    `json:"txn"`
	PairID          string    `json:"pair_id"`
	Platform        string    `json:"platform"`
	ProgramAddress  string    `json:"program_address"`
	BasePostAmount  float64   `json:"base_post_amount"`
	QuotePostAmount float64   `json:"quote_post_amount"`
	BondingCurve    *string   `json:"bonding_curve"`
	BaseAddress     string    `json:"base_address"`
	QuoteAddress    string    `json:"quote_address"`
	LiquiditySOL    float64   `json:"liquidity_sol"`
	LiquidityToken  float64   `json:"liquidity_token"`
	LiquidityUSD    float64   `json:"liquidity_usd"`
	Start1M         time.Time `json:"start_1m"`
	Start5M         time.Time `json:"start_5m"`
	Start15M        time.Time `json:"start_15m"`
	Start30M        time.Time `json:"start_30m"`
	Start1H         time.Time `json:"start_1h"`
	Start4H         time.Time `json:"start_4h"`
	Start6H         time.Time `json:"start_6h"`
	Start12H        time.Time `json:"start_12h"`
	Start24H        time.Time `json:"start_24h"`
	Start1Month     time.Time `json:"start_1M"`
	Open            float64   `json:"open"`
	High            float64   `json:"high"`
	Low             float64   `json:"low"`
	Close           float64   `json:"close"`
	Volume          float64   `json:"volume"`
	ChainNetwork    string    `json:"chain_network"`
	Index           float64   `json:"index"`
}

// UnmarshalJSON implements custom JSON unmarshaling to handle bonding_curve as number, string, or null
func (t *Transaction) UnmarshalJSON(data []byte) error {
	// Use a temporary struct with interface{} for bonding_curve
	type Alias Transaction
	aux := &struct {
		BondingCurve interface{} `json:"bonding_curve"`
		*Alias
	}{
		Alias: (*Alias)(t),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// Handle bonding_curve: can be null, string, or number
	if aux.BondingCurve == nil {
		t.BondingCurve = nil
	} else {
		var bondingCurveStr string
		switch v := aux.BondingCurve.(type) {
		case string:
			bondingCurveStr = v
		case float64:
			// Convert number to string
			bondingCurveStr = strconv.FormatFloat(v, 'f', -1, 64)
		case int:
			bondingCurveStr = strconv.Itoa(v)
		case int64:
			bondingCurveStr = strconv.FormatInt(v, 10)
		default:
			// Try to convert to string
			bondingCurveStr = fmt.Sprintf("%v", v)
		}
		t.BondingCurve = &bondingCurveStr
	}

	return nil
}
