package config

import (
	_ "embed"
	"encoding/json"
	"fmt"
)

//go:embed config.json
var configData []byte

type Config struct {
	BlockSize   int    `json:"BLOCK_SIZE"`
	SummaryStep int    `json:"SUMMARY_STEP"`
	Tombstone      string  `json:"TOMBSTONE"`
	TokenRefillRate float64 `json:"TOKEN_REFILL_RATE"`
	MaxTokens   int    `json:"MAX_TOKEN"`
	MemtableType string `json:"MEMTABLE_TYPE"`
	MemtableCount int    `json:"MEMTABLE_COUNT"`
	MemtableSize  int    `json:"MEMTABLE_SIZE"`
}

func GetConfig() Config {
	var config Config
	if err := json.Unmarshal(configData, &config); err != nil {
		panic(fmt.Sprintf("failed to parse config file: %v", err))
	}

	return config
}
