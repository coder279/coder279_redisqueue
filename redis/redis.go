package redis

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/pkg/errors"
	"regexp"
	"strconv"
	"strings"
)

var redisVersionRE = regexp.MustCompile(`redis_version:(.+)`)

type RedisOption = redis.Options

//实例化Redis配置
func NewRedisClient(options *RedisOption) *redis.Client {
	if options == nil {
		options = &RedisOption{
			Addr:     "localhost:6379", // redis地址
			Password: "",               // redis密码，没有则留空
			DB:       0,                // 默认数据库，默认是0
		}
	}
	return redis.NewClient(options)
}

//Redis版本检查
func RedisVersionCheck(client redis.UniversalClient) error {
	info, err := client.Info("server").Result()
	if err != nil {
		return err
	}
	match := redisVersionRE.FindAllStringSubmatch(info, -1)
	if len(match) < 1 {
		return fmt.Errorf("could not extract redis version")
	}
	version := strings.TrimSpace(match[0][1])
	parts := strings.Split(version, ".")
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return err
	}
	if major < 5 {
		return fmt.Errorf("redis streams are not supported in version %q", version)
	}
	return nil
}
//新增ID序号 xx-0 xx-1的形式
func IncrementMessageId(id string)(string,error){
	parts := strings.Split(id,",")
	index := parts[1]//页码
	parsed, err := strconv.ParseInt(index, 10, 64)
	if err != nil {
		return "", errors.Wrapf(err, "error parsing message ID %q", id)
	}
	return fmt.Sprintf("%s-%d",parts[0],parsed+1),nil
}
