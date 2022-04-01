package main

import (
	"fmt"
	"github.com/coder279/coder279_redisqueue/common"
	"time"
)

func PrintMsgMap(msgMap map[string]map[string][]string) (key2msgIds map[string][]string, msgCount int32){
	key2msgIds = make(map[string][]string, 0)
	msgCount = 0
	for streamKey, val := range msgMap {
		//fmt.Println("StreamKey:", streamKey)
		vecMsgId := make([]string, 0)
		for msgId, msgList := range val {
			//fmt.Println("MsgId:", msgId)
			vecMsgId = append(vecMsgId, msgId)
			for msgIndex := 0; msgIndex < len(msgList); msgIndex = msgIndex + 2 {
				//var msgKey = msgList[msgIndex]
				//var msgVal = msgList[msgIndex+1]
				msgCount++
				//fmt.Println("MsgKey:", msgKey, "MsgVal:", msgVal)
			}
		}
		key2msgIds[streamKey] = vecMsgId
	}
	return
}

// 非阻塞无消费组的情况
func testNoGroup(redisCli *common.RedisStreamMQClient) {

	startTime := time.Now()
	fmt.Println("Start Test Function GetMsg")
	msgMap, err2 := redisCli.GetMsg(common.READ_MSG_AMOUNT, common.TEST_STREAM_KEY, "0")
	if err2 != nil {
		fmt.Println("GetMsg Failed. streamKey:", common.TEST_STREAM_KEY, "err:", err2)
		return
	}
	_, msgCount := PrintMsgMap(msgMap)
	fmt.Println("streamKey:", common.TEST_STREAM_KEY, "Ack Msg Count:", msgCount)
	fmt.Println("End Test Function GetMsg")

	costTime := time.Since(startTime)
	fmt.Println("=========:START TIME:", startTime)
	fmt.Println("=========:COST TIME:", costTime)
}

// 阻塞模式无消费组的情况
func testNoGroupBlock(redisCli *common.RedisStreamMQClient) {

	startTime := time.Now()
	fmt.Println("Start Test Function GetMsgBlock")
	msgMap, err := redisCli.GetMsgBlock(common.READ_MSG_BLOCK_SEC, common.READ_MSG_AMOUNT, common.TEST_STREAM_KEY)
	if err != nil {
		fmt.Println("GetMsg Failed. streamKey:", common.TEST_STREAM_KEY, "err:", err)
		return
	}
	_, msgCount := PrintMsgMap(msgMap)
	fmt.Println("streamKey:", common.TEST_STREAM_KEY, "Ack Msg Count:", msgCount)
	fmt.Println("End Test Function GetMsgBlock")

	costTime := time.Since(startTime)
	fmt.Println("=========:START TIME:", startTime)
	fmt.Println("=========:COST TIME:", costTime)
}

// 非阻塞消费
func testGroupConsumer(redisCli *common.RedisStreamMQClient, groupName string, consumerName string, msgAmount int32){

	/*
		fmt.Println("Start Test Function CreateConsumerGroup")
		err3 := redisCli.CreateConsumerGroup(common.TEST_STREAM_KEY, groupName, "0")
		if err3 != nil {
			fmt.Println("CreateConsumerGroup Failed. err:", err3)
			return
		}
		fmt.Println("End Start Test Function CreateConsumerGroup")
	*/
	startTime := time.Now()
	fmt.Println("Start Test Function GetMsgByGroupConsumer")
	msgMap3, err3 := redisCli.GetMsgByGroupConsumer(common.TEST_STREAM_KEY, groupName, consumerName, msgAmount)

	if err3 != nil {
		fmt.Println("GetMsgByGroupConsumer Failed. err:", err3)
		return
	}
	fmt.Println("End Test Function GetMsgByGroupConsumer")

	fmt.Println("Start Test Function ReplyAck")
	key2msgIds3, _ := PrintMsgMap(msgMap3)
	for streamKey, vecMsgId := range key2msgIds3 {
		//fmt.Println("streamKey:", streamKey, "groupName:", groupName, "consumerName:", consumerName, "Ack Msg Count:", msgCount)
		err3 = redisCli.ReplyAck(streamKey, groupName, vecMsgId)
		if err3 != nil {
			fmt.Println("ReplyAck Failed. err:", err3)
		}
	}

	fmt.Println("End Test Function ReplyAck")
	costTime := time.Since(startTime)
	fmt.Println("=========:START TIME:", startTime)
	fmt.Println("=========:COST TIME:", costTime)
}

// 阻塞消费
func testGroupConsumerBlock(redisCli *common.RedisStreamMQClient, groupName string, consumerName string, msgAmount int32){

	/*
		fmt.Println("Start Test Function CreateConsumerGroup")
		err3 := redisCli.CreateConsumerGroup(common.TEST_STREAM_KEY, groupName, "0")
		if err3 != nil {
			fmt.Println("CreateConsumerGroup Failed. err:", err3)
			return
		}
		fmt.Println("End Start Test Function CreateConsumerGroup")
	*/

	startTime := time.Now()
	fmt.Println("Start Test Function GetMsgBlockByGroupConsumer")
	msgMap6, err3 := redisCli.GetMsgBlockByGroupConsumer(common.READ_MSG_BLOCK_SEC,
		common.TEST_STREAM_KEY, groupName, "ConsumerName1-A", msgAmount)

	if err3 != nil {
		fmt.Println("GetMsgBlockByGroupConsumer Failed. err:", err3)
		return
	}
	fmt.Println("End Test Function GetMsgBlockByGroupConsumer")

	fmt.Println("Start Test Function ReplyAck")
	key2msgIds6, msgCount := PrintMsgMap(msgMap6)
	for streamKey, vecMsgId := range key2msgIds6 {
		fmt.Println("streamKey:", streamKey, "groupName:", groupName, "consumerName:", consumerName, "Ack Msg Count:", msgCount)
		err3 = redisCli.ReplyAck(streamKey, groupName, vecMsgId)
		if err3 != nil {
			fmt.Println("ReplyAck Failed. err:", err3)
		}
	}
	fmt.Println("End Test Function ReplyAck")
	costTime := time.Since(startTime)
	fmt.Println("=========:START TIME:", startTime)
	fmt.Println("=========:COST TIME:", costTime)
}

func testPendingList(redisCli *common.RedisStreamMQClient, groupName string, consumerName string){

	fmt.Println("Start Test Function GetPendingList")
	vecPendingMsg, _ := redisCli.GetPendingList(common.TEST_STREAM_KEY, groupName, consumerName, common.READ_MSG_AMOUNT)

	vecMsgId := make([]string, 0)
	for _, pendingMsg := range vecPendingMsg {
		vecMsgId = append(vecMsgId, pendingMsg.MsgId)
	}
	_ = redisCli.ReplyAck(common.TEST_STREAM_KEY, groupName, vecMsgId)
	fmt.Println("Start Test Function GetPendingList")
}

func testXinfo(redisCli *common.RedisStreamMQClient, streamKey string, groupName string) {
	fmt.Println("Start Test Function MonitorMqInfo")
	streamMqInfo := redisCli.MonitorMqInfo(streamKey)
	fmt.Println("streamMqInfo:{")
	fmt.Println("Length:", streamMqInfo.Length)
	fmt.Println("RedixTreeKeys:", streamMqInfo.RedixTreeKeys)
	fmt.Println("RedixTreeNodes:",streamMqInfo.RedixTreeNodes)
	fmt.Println("LastGeneratedId:", streamMqInfo.LastGeneratedId)
	fmt.Println("Groups:",streamMqInfo.Groups)
	fmt.Println("FirstEntry:", streamMqInfo.FirstEntry)
	fmt.Println("LastEntry:", streamMqInfo.LastEntry)
	fmt.Println("}")
	fmt.Println("End Test Function MonitorMqInfo")

	fmt.Println("Start Test Function MonitorConsumerGroupInfo")
	groupInfo := redisCli.MonitorConsumerGroupInfo(streamKey)
	if groupInfo != nil {
		fmt.Println("groupInfo:{")
		fmt.Println("Name:", groupInfo.Name)
		fmt.Println("Consumers:", groupInfo.Consumers)
		fmt.Println("Pending:", groupInfo.Pending)
		fmt.Println("LastDeliveredId:", groupInfo.LastDeliveredId)
		fmt.Println("}")
	}
	fmt.Println("End Test Function MonitorConsumerGroupInfo")

	fmt.Println("Start Test Function MonitorConsumerInfo")
	vecConsumerInfo := redisCli.MonitorConsumerInfo(streamKey, groupName)
	fmt.Println("groupInfo:{")
	for _, consumerInfo := range vecConsumerInfo {
		fmt.Println("Name:", consumerInfo.Name)
		fmt.Println("Pending:", consumerInfo.Pending)
		fmt.Println("Idle:", consumerInfo.Idle)
		fmt.Println("===========================")
	}
	fmt.Println("}")
	fmt.Println("End Test Function MonitorConsumerInfo")
}

func main() {

	fmt.Println("test redis stream mq consumer")
	redisOpt := common.RedisConnOpt{
		Enable: true,
		Host:   "127.0.0.1",
		Port:   6379,
		TTL:    240,
	}

	redisCli := common.NewClient(redisOpt)
	fmt.Println("Test Redis Consumer Client Host:", redisCli.RedisConnOpt.Host,
		", Port:", redisCli.RedisConnOpt.Port, ", DB:", redisCli.RedisConnOpt.Index)

	//无消费者组，所有消费者都能消费所有消息
	//testNoGroup(redisCli)

	//无消费者组，所有消费者都能消费所有消息(阻塞模式)
	//testNoGroupBlock(redisCli)

	//有消费者组，所有消费者都不能重复消费组内的消息
	var groupName1 string = "GroupName1"
	var consumerName1 string = "ConsumerName1-" + time.Now().String()
	var msgAmount1 int32 = 50000
	testGroupConsumer(redisCli, groupName1, consumerName1, msgAmount1)
	testPendingList(redisCli, groupName1, consumerName1)

	//有消费者组，所有消费者都不能重复消费组内的消息(阻塞模式)
	//var groupName2 string = "GroupName2"
	//var consumerName2 string = "ConsumerName2-" + time.Now().String()
	//var msgAmount2 int32 = 50000
	//testGroupConsumerBlock(redisCli, groupName2, consumerName2, msgAmount2)
	//testPendingList(redisCli, groupName2, consumerName2)

	//XINFO测试
	//var groupName1 string = "testgroupname"
	//var streamKey string = "test-mq"
	//testXinfo(redisCli, streamKey, groupName1)

	return
}
