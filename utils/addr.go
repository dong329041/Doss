package utils

import (
	"net"
	"os"
	"strings"

	"config"
)

// 获取rabbitMQ的连接地址
func GetRabbitMqUrl() (rabbitMQUrl string) {
	if os.Getenv("DOSS_RABBIT_MQ_URL") != "" {
		rabbitMQUrl = os.Getenv("DOSS_RABBIT_MQ_URL")
		return
	}
	rabbitMQUrl = config.GConfig.RabbitMQUrl
	return
}

// 获取MongoDB的连接地址
func GetMongodbUrl() (mongodbUrl string) {
	if os.Getenv("DOSS_MONGO_URL") != "" {
		mongodbUrl = os.Getenv("DOSS_MONGO_URL")
		return
	}
	mongodbUrl = config.GConfig.MongodbUrl
	return
}

// 获取MongoDB的ip地址和端口号
// 单机模式：ip:port，或者副本集方式：ip:port,ip:port,ip:port
func GetMongodbIpPort() (mongoIpPort []string) {
	var (
		splitArray []string
		ipPort     string
		ipArray    []string
	)

	// url为：mongodb://username:password@ip:port[,ip:port][,ip:port]
	splitArray = strings.Split(GetMongodbUrl(), "@")

	// url为：mongodb://ip:port[,ip:port][,ip:port]
	if len(splitArray) == 1 {
		splitArray = strings.Split(GetMongodbUrl(), "://")
	}
	ipPort = splitArray[len(splitArray)-1]
	mongoIpPort = strings.Split(ipPort, ",")

	// url为：mongodb://localhost:port
	if strings.Contains(ipPort, "localhost") {
		splitArray = strings.Split(ipPort, ":")
		ipArray, _ = GetAllLocalIp()
		if len(ipArray) > 0 {
			mongoIpPort = []string{}
			mongoIpPort = append(mongoIpPort, ipArray[0]+":"+splitArray[len(splitArray)-1])
		}
	}

	return
}

// 获取本机的ip地址 (向MongoDB主机发起通信的ip地址)
func GetLocalIp() (localIp string) {
	var (
		conn    net.Conn
		ipArray []string
		err     error
	)

	if conn, err = net.Dial("udp", GetMongodbIpPort()[0]); err != nil || conn == nil {
		ipArray, err = GetAllLocalIp()
		if err != nil && len(ipArray) > 0 {
			localIp = ipArray[0]
			return
		}
		localIp = ""
		return
	}
	defer conn.Close()

	ipArray, err = GetAllLocalIp()
	localIp = strings.Split(conn.LocalAddr().String(), ":")[0]
	return localIp
}

// 获取本机所有的网卡ip地址
func GetAllLocalIp() (ipArray []string, err error) {
	var (
		addrArray []net.Addr
	)

	if addrArray, err = net.InterfaceAddrs(); err != nil {
		return
	}
	for _, address := range addrArray {
		if ipNet, ok := address.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				ipArray = append(ipArray, ipNet.IP.String())
			}
		}
	}
	return
}
