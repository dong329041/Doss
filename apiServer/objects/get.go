package objects

import (
	"common"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"apiServer/heartbeat"
	"config"
	"hashRing"
	"meta"
	"meta/funcParams"
	"stream"
	"utils"
)

func get(w http.ResponseWriter, r *http.Request) {
	var (
		Meta      *meta.ObjectMeta
		name      string
		qVersion  []string
		version   int
		getStream *stream.RSGetStream
		offset    int64
		err       error
	)

	// 提交参数检查（name、version）
	name = strings.Split(r.URL.EscapedPath(), "/")[2]
	qVersion = r.URL.Query()["version"]
	version = 0
	if len(qVersion) != 0 {
		if version, err = strconv.Atoi(qVersion[0]); err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	// 获取相应版本的对象元数据
	// 1) 若url中未加version查询参数，则返回最新的版本；
	// 2) 若url中存在version查询参数，则返回对应的版本；若不存在该版本，返回http.StatusNotFound
	if version == 0 {
		Meta, err = meta.NewDossMongo().GetObjectMeta(name)
	} else {
		Meta, err = meta.NewDossMongo().GetObjectMeta(name, funcParams.MetaParamVersion(version))
	}
	if err != nil {
		log.Println("Get object meta error: ", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if Meta.Hash == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// 生成对象下载流
	if getStream, err = GetStream(Meta); err != nil {
		log.Println("GetRSStream error:", err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// 从请求头获取对象读取偏移量，并移动对象下载流的seek指针
	if offset = utils.GetOffsetFromHeader(r.Header); offset != 0 {
		_ = getStream.Seek(offset, io.SeekCurrent)
		w.Header().Set("content-range", fmt.Sprintf("bytes %d-%d/%d", offset, Meta.Size-1, Meta.Size))
		w.WriteHeader(http.StatusPartialContent)
	}

	// 将对象数据流式拷贝到http.ResponseWriter
	io.Copy(w, getStream)

	// 调用Close方法将GetStream中分片修复的数据流提交转正，dataServer将临时对象转为正式对象
	getStream.Close()
}

// 生成数据下载流
func GetStream(Meta *meta.ObjectMeta) (getStream *stream.RSGetStream, err error) {
	var (
		locateInfo  map[int]string
		dataServers []string
		nodes       []string
		node        string
		index       int
		i           int
	)

	// 获取数据的定位节点
	if nodes, err = hashRing.GetNodes(Meta.Hash, config.GConfig.AllShards); err != nil {
		log.Println(common.ErrDataLocate, err)
		return
	}

	// 获取在线的数据节点集合
	dataServers = heartbeat.GetOnlineDataServers()

	// 处理定位节点（只获取当前在线的节点，宕机节点略过）
	locateInfo = make(map[int]string)
	for i, node = range nodes {
		if index = utils.SliceIndexOfMember(dataServers, node); index != -1 {
			locateInfo[i] = dataServers[index]
		}
	}

	// 返回用于纠删码读取的下载数据流
	return stream.NewRSGetStream(locateInfo, Meta.Hash, Meta.Size)
}
