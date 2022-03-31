package main

import (
	"fmt"
	"github.com/coder279/coder279_redisqueue/redis"
	rdb "github.com/go-redis/redis/v7"
)

func main() {
	options := &rdb.Options{
		Addr:     "localhost:6379", // redis地址
		Password: "", // redis密码，没有则留空
		DB:       0,  // 默认数据库，默认是0
	}
	client := redis.NewRedisClient(options)
	err := redis.RedisVersionCheck(client)
	if err != nil{
		fmt.Println(err)
	}
	fmt.Println("success")

}
