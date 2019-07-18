package heartbeat

import (
	"common"
	"log"
	"time"

	"config"
	"meta"
	"meta/funcParams"
	"rbmq"
	"utils"
)

// 开始向apiServer汇报心跳消息
func StartHeartbeat(ListenIp, ListenPort string) {
	var mq *rbmq.Producer
	mq = rbmq.NewProducer(utils.GetRabbitMqUrl())
	defer mq.Close()
	for {
		mq.Publish(config.GConfig.HeartbeatExchange, ListenIp+":"+ListenPort)
		time.Sleep(config.GConfig.HeartbeatInterval * time.Second)
	}
}

// 将当前节点IP和weight注册到MongoDB数据库中
func RegisterNodeToDB(ListenIp string, Weight int) {
	var (
		DMongo *meta.DossMongo
		err    error
	)

	// 将数据节点注册到MongoDB数据库中
	DMongo = meta.NewDossMongo(
		funcParams.MongoParamCollection(config.GConfig.NodeColName),
	)
	if _, err = DMongo.AddDsNode(ListenIp, Weight); err != nil {
		log.Fatal(common.ErrRegisterNode, err)
	}
}
