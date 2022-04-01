package common

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"log"
	"time"
)

func NewClient(opt RedisConnOpt) *RedisStreamMQClient {
	return &RedisStreamMQClient{
		RedisConnOpt: opt,
		ConnPool:     newPool(opt),
	}
}

func newPool(opt RedisConnOpt) *redis.Pool{
	return &redis.Pool{
		MaxIdle: 3,
		IdleTimeout: 240*time.Second,
		MaxActive: 10,
		Wait: true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", fmt.Sprintf("%s:%d", opt.Host, opt.Port))
			if err != nil {
				log.Fatalf("Redis.Dial: %v", err)
				return nil, err
			}
			/*
				if _, err := c.Do("AUTH", opt.Password); err != nil {
					c.Close()
					log.Fatalf("Redis.AUTH: %v", err)
					return nil, err
				}
			*/
			if _, err := c.Do("SELECT", opt.Index); err != nil {
				c.Close()
				log.Fatalf("Redis.SELECT: %v", err)
				return nil, err
			}
			return c, nil
		},
	}
}
