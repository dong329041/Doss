package config

import (
	"common"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"time"
)

// 程序全局配置
type Config struct {
	ApiServerPort       int           `json:"apiServerPort"`
	DataServerPort      int           `json:"dataServerPort"`
	HeartbeatInterval   time.Duration `json:"heartbeatInterval"`
	HeartbeatOverTime   time.Duration `json:"heartbeatOverTime"`
	DataServerWeight    int           `json:"dataServerWeight"`
	DefaultVirtualCubes int           `json:"defaultVirtualCubes"`
	AggregateObjSize    int64         `json:"aggregateObjSize"`
	DataShards          int           `json:"dataShards"`
	ParityShards        int           `json:"parityShards"`
	AllShards           int
	BlockSize           int
	BlockPerShard       int           `json:"blockPerShard"`
	RabbitMQUrl         string        `json:"rabbitMQUrl"`
	ExchangeType        string        `json:"exchangeType"`
	HeartbeatExchange   string        `json:"heartbeatExchange"`
	MongodbUrl          string        `json:"mongodbUrl"`
	MongoConnectTimeout time.Duration `json:"mongodbConnectTimeout"`
	DatabaseName        string        `json:"databaseName"`
	ObjectColName       string        `json:"objectColName"`
	AggregateObjColName string        `json:"aggregateObjColName"`
	ObjShardColName     string        `json:"objShardColName"`
	RepairObjColName    string        `json:"repairObjColName"`
	NodeColName         string        `json:"nodeColName"`
	JwtSecretKey        string        `json:"jwtJwtSecretKey"`
}

var (
	GConfig  *Config // 全局配置（单例模式）
	ConfPath = flag.String("config", "/etc/doss/config.json", "config file path")
)

// 在导入此包进行初始化时，进行全局配置的初始化
func init() {
	if err := InitConfig(*ConfPath); err != nil {
		log.Println(common.ErrParseConfig, err)
		os.Exit(-1)
	}
}

// 加载配置
func InitConfig(file string) (err error) {
	var (
		content []byte
		conf    Config
	)

	// 解析配置文件
	if content, err = ioutil.ReadFile(file); err != nil {
		return
	}
	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}
	conf.AllShards = conf.DataShards + conf.ParityShards
	conf.BlockSize = conf.BlockPerShard * conf.DataShards

	GConfig = &conf
	return
}
