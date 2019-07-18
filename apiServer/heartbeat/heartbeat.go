package heartbeat

import (
	"strconv"
	"sync"
	"time"

	"config"
	"github.com/streadway/amqp"
	"rbmq"
	"utils"
)

var dataServers = make(map[string]time.Time)
var rwMutex sync.RWMutex

// 监听数据节点发送的心跳
func ListenHeartbeat() {
	var (
		mq           *rbmq.Consumer
		deliveryChan <-chan amqp.Delivery
		dataServer   string
		err          error
	)

	mq = rbmq.NewConsumer(utils.GetRabbitMqUrl())
	defer mq.Close()
	mq.DeclareExchange(config.GConfig.HeartbeatExchange)
	mq.QueueBind(config.GConfig.HeartbeatExchange)
	deliveryChan = mq.Consume()

	go checkDataServers()

	for msg := range deliveryChan {
		dataServer, err = strconv.Unquote(string(msg.Body))
		if err != nil {
			panic(err)
		}
		rwMutex.Lock()
		dataServers[dataServer] = time.Now()
		rwMutex.Unlock()
	}
}

// 定期检查数据节点发送的心跳情况：将超过阈值没有收到心跳的数据节点从map中移除
func checkDataServers() {
	var (
		dataServer string
		timestamp  time.Time
	)

	for {
		time.Sleep(config.GConfig.HeartbeatInterval * time.Second)
		rwMutex.Lock()
		for dataServer, timestamp = range dataServers {
			if timestamp.Add(config.GConfig.HeartbeatOverTime * time.Second).Before(time.Now()) {
				delete(dataServers, dataServer)
			}
		}
		rwMutex.Unlock()
	}
}

// 获取在线的数据节点集合
func GetOnlineDataServers() []string {
	var (
		ds           string
		dsCollection []string
	)

	rwMutex.RLock()
	defer rwMutex.RUnlock()
	dsCollection = make([]string, 0)
	for ds = range dataServers {
		dsCollection = append(dsCollection, ds)
	}
	return dsCollection
}
