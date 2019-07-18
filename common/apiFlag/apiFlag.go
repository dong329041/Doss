package apiFlag

import (
	"flag"

	"config"
	"utils"
)

// apiServer程序监听的ip
var ListenIp = flag.String("listen_ip", utils.GetLocalIp(), "apiServer's listen ip")

// apiServer程序监听的port
var ListenPort = flag.Int("listen_port", config.GConfig.ApiServerPort, "apiServer's listen port")

// 初始化，解析命令行参数
func init() {
	flag.Parse()
}
