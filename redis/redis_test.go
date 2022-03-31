package redis

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewRedisClient(t *testing.T) {
	t.Run("returns a new redis client", func(t *testing.T) {
		options := &RedisOption{
			Addr:     "localhost:6379", // redis地址
			Password: "", // redis密码，没有则留空
			DB:       0,  // 默认数据库，默认是0
		}
		r := NewRedisClient(options)
		err := r.Ping().Err()
		assert.NoError(t, err)
	})

	t.Run("defaults options if it's nil", func(tt *testing.T) {
		r := NewRedisClient(nil)
		err := r.Ping().Err()
		assert.NoError(tt, err)
	})
}
// 错误校验
func TestRedisVersionCheck(t *testing.T) {
	t.Run("bubbles up errors", func(t *testing.T) {
		options := &RedisOption{
			Addr:     "localhost:0", // redis地址
			Password: "", // redis密码，没有则留空
			DB:       0,  // 默认数据库，默认是0
		}
		r := NewRedisClient(options)
		err := RedisVersionCheck(r)
		require.Error(t, err)
		assert.Contains(t, err.Error(),"dial tcp")
	})
}
