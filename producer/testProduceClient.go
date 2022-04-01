package main

import (
	"fmt"
	"github.com/coder279/coder279_redisqueue/common"
	"time"
)

func testPutMsg(redisCli *common.RedisStreamMQClient, textKey string, textVal string, msgCount int) {

	startTime := time.Now()
	fmt.Println("Start Test Function testPutMsg")
	for i := 0; i < msgCount; i++ {
		var strMsgKey string = fmt.Sprintf("%s-%d", textKey, i+1)
		var strMsgVal string = fmt.Sprintf("%s-%d", textVal, i+1)
		_, err := redisCli.PutMsg(common.TEST_STREAM_KEY, strMsgKey, strMsgVal)
		if err != nil {
			fmt.Println("PutMsg Failed. err:", err)
		}
	}
	fmt.Println("End Test Function testPutMsg")

	costTime := time.Since(startTime)
	fmt.Println("=========:START TIME:", startTime)
	fmt.Println("=========:COST TIME:", costTime)
}

func testPutMsgBatch(redisCli *common.RedisStreamMQClient, textKey string, textVal string, msgCount int) {

	startTime := time.Now()
	fmt.Println("Start Test Function PutMsgBatch")
	msgMap := make(map[string]string,0)
	for i := 0; i < msgCount; i++ {
		var strMsgKey string = fmt.Sprintf("%s-%d", textKey, i+1)
		var strMsgVal string = fmt.Sprintf("%s-%d", textVal, i+1)
		msgMap[strMsgKey] = strMsgVal
	}
	vecMsgId, err2 := redisCli.PutMsgBatch(common.TEST_STREAM_KEY, msgMap)
	if err2 != nil {
		fmt.Println("PutMsgBatch Failed. err:", err2)
	}
	fmt.Println("Reply Msg Id:", vecMsgId)
	fmt.Println("End Test Function PutMsgBatch")

	costTime := time.Since(startTime)
	fmt.Println("=========:START TIME:", startTime)
	fmt.Println("=========:COST TIME:", costTime)
}

func main() {

	fmt.Println("test redis stream mq producer")
	redisOpt := common.RedisConnOpt{
		Enable: true,
		Host:   "127.0.0.1",
		Port:   6379,
		TTL:    240,
	}

	redisCli := common.NewClient(redisOpt)
	fmt.Println("Test Redis Producer Client Host:", redisCli.RedisConnOpt.Host,
		", Port:", redisCli.RedisConnOpt.Port, ", DB:", redisCli.RedisConnOpt.Index)

	// 单条生产消息
	var textKey string = "DEMO-TEST-STREAM-MSG-KEY"
	var textVal string = "DEMO-TEST-STREAM-MSG-VAL"
	var msgCount int = 100
	testPutMsg(redisCli, textKey, textVal, msgCount)

	// 批量生产消息
	//var textKey2 string = "DEMO-TEST-STREAM-MSG-KEY"
	//var textVal2 string = "DEMO-TEST-STREAM-MSG-VAL"
	//var msgCount2 int = 500
	//testPutMsgBatch(redisCli, textKey2, textVal2, msgCount2)
	//testPutMsgBatch(redisCli, textKey2, textVal2, msgCount2)
	//testPutMsgBatch(redisCli, textKey2, textVal2, msgCount2)

	return
}
