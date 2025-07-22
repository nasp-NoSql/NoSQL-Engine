package token_bucket

import (
	"fmt"
	"nosqlEngine/src/config"
	"time"
)

var CONFIG = config.GetConfig()

type TokenBucket struct {
	currTokens     int
	lastRefillTime int64
}

func GetNewTokenBucket() *TokenBucket{
	return &TokenBucket{currTokens: CONFIG.MaxTokens, lastRefillTime: time.Now().Unix()}
}

func (tb *TokenBucket) CheckTokens() (bool, error) {
	// Implement token checking logic here
	curr_tokens := tb.currTokens
	last_refill_time := tb.lastRefillTime
	now := time.Now().Unix()
	elapsed_time := float64(now - last_refill_time)
	new_tokens := int(elapsed_time * CONFIG.TokenRefillRate) // floor to int
	curr_tokens = min(new_tokens+curr_tokens, CONFIG.MaxTokens)
	
	if curr_tokens < 1 {
		return false, fmt.Errorf("insufficient tokens")
	}
	tb.currTokens = curr_tokens - 1
	return true, nil
}

