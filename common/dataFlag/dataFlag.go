package dataFlag

import (
	"config"
	"flag"
	"os"
	"utils"
)

// dataServer程序监听的ip
var ListenIp = flag.String("listen_ip", utils.GetLocalIp(), "dataServer's listen ip")

// dataServer程序监听的port
var ListenPort = flag.Int("listen_port", config.GConfig.DataServerPort, "dataServer's listen port")

// 数据节点的存储空间权重（默认是1）
var Weight = flag.Int("weight", config.GConfig.DataServerWeight, "dataServer's weight")

// 数据节点的数据根目录
var StorageRoot = flag.String("storage_root", os.Getenv("DOSS_STORAGE_ROOT"), "dataServer's storage root path")

// 初始化，解析命令行参数
func init() {
	flag.Parse()
}
