package main

import (
	"log"
	"net/http"
	"runtime"
	"strconv"

	"apiServer/heartbeat"
	"apiServer/locate"
	"apiServer/objects"
	"apiServer/temp"
	"apiServer/versions"
	"common/apiFlag"
	"hashRing"
)

// 程序初始化：设置线程数量
func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	go heartbeat.ListenHeartbeat()
	go hashRing.CheckHashRing()
	go objects.ListenObjectsRepair()

	http.HandleFunc("/objects/", objects.Handler)
	http.HandleFunc("/temp/", temp.Handler)
	http.HandleFunc("/locate/", locate.Handler)
	http.HandleFunc("/versions/", versions.Handler)

	log.Fatal(http.ListenAndServe(*apiFlag.ListenIp+":"+strconv.Itoa(*apiFlag.ListenPort), nil))
}
