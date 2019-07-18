package main

import (
	"log"
	"net/http"
	"runtime"
	"strconv"

	"common/dataFlag"
	"dataServer/check"
	"dataServer/heartbeat"
	"dataServer/locate"
	"dataServer/objects"
	"dataServer/temp"
)

// 将本机监听ip和端口注册到数据库中、设置线程数量
func init() {
	heartbeat.RegisterNodeToDB(*dataFlag.ListenIp, *dataFlag.Weight)
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	go locate.CollectObjects(*dataFlag.StorageRoot)
	go locate.CollectAggObjects(*dataFlag.StorageRoot)
	go check.SystemDataCheck()
	go heartbeat.StartHeartbeat(*dataFlag.ListenIp, strconv.Itoa(*dataFlag.ListenPort))
	http.HandleFunc("/locate/", locate.Handler)
	http.HandleFunc("/objects/", objects.Handler)
	http.HandleFunc("/temp/", temp.Handler)

	log.Fatal(http.ListenAndServe(*dataFlag.ListenIp+":"+strconv.Itoa(*dataFlag.ListenPort), nil))
}
