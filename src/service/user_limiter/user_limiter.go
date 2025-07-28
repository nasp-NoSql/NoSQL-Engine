package user_limiter

import (
	"nosqlEngine/src/config"
	"nosqlEngine/src/service/token_bucket"
)

var CONFIG = config.GetConfig()

type UserLimiter struct {
	data   map[string]*token_bucket.TokenBucket
}
func NewUserLimiter() *UserLimiter {
	return &UserLimiter{
		data: make(map[string]*token_bucket.TokenBucket),
	}
}

func (ul *UserLimiter) CheckUserTokens(user string) (bool, error) {
	if _, exists := ul.data[user]; !exists {
		ul.data[user] = token_bucket.GetNewTokenBucket()
	}
	return ul.data[user].CheckTokens()
}