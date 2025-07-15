package config

import (
	_ "embed"
	"encoding/json"
	"fmt"
)

//go:embed config.json
var configData []byte

type Config struct {
	BlockSize   int `json:"BLOCK_SIZE"`
	SummaryStep int `json:"SUMMARY_STEP"`
	Tombstone   string `json:"TOMBSTONE"`
}

func GetConfig() Config {
	var config Config
	if err := json.Unmarshal(configData, &config); err != nil {
		panic(fmt.Sprintf("failed to parse config file: %v", err))
	}

	return config
}