package config

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	blockSize int `json:"block_size"`
}

func getConfig() Config {
	configFile, _ := ioutil.ReadFile("nosqlEngine/src/config/config")
	var config Config
	err = json.Unmarshal(configFile, &config)

	return config
}