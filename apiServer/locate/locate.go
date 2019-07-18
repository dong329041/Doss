package locate

import (
	"common"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"

	"apiServer/heartbeat"
	"config"
	"hashRing"
	"utils"
)

func Locate(elmName string) (locateInfo map[int]string) {
	var (
		dataServers []string
		nodes       []string
		node        string
		index       int
		request     *http.Request
		response    *http.Response
		resData     []byte
		shardIndex  int
		err         error
	)

	// 获取数据的定位节点
	if nodes, err = hashRing.GetNodes(elmName, config.GConfig.AllShards); err != nil {
		log.Fatal(common.ErrDataLocate, err)
		return
	}

	// 获取在线的数据节点集合
	dataServers = heartbeat.GetOnlineDataServers()

	// 返回定位信息
	locateInfo = make(map[int]string)
	for _, node = range nodes {
		if index = utils.SliceIndexOfMember(dataServers, node); index != -1 {
			// 向数据节点发送GET数据定位请求
			request, err = http.NewRequest("GET", "http://"+dataServers[index]+"/locate/"+elmName, nil)
			if err != nil {
				continue
			}
			client := http.Client{}
			if response, err = client.Do(request); err != nil {
				continue
			}
			if resData, err = ioutil.ReadAll(response.Body); err != nil || len(resData) == 0 {
				continue
			}
			// 解析各分片所在数据节点
			if shardIndex, err = strconv.Atoi(string(resData)); err != nil || shardIndex == -1 {
				continue
			}
			locateInfo[shardIndex] = node
		}
	}
	return
}

// 获取元素定位的节点集合（只获取在线的节点）
func GetLocateNodes(name string) (Nodes []string, err error) {
	var (
		Node        string
		OnlineNodes []string
		OnlineNode  string
		index       int
		isOnline    bool
	)

	// 获取对象定位的数据节点集合
	Nodes, _ = hashRing.GetNodes(name, config.GConfig.AllShards)
	if len(Nodes) != config.GConfig.AllShards {
		err = common.ErrNotEnoughDS
		return
	}

	// 获取在线的数据节点
	OnlineNodes = heartbeat.GetOnlineDataServers()
	for index, Node = range Nodes {
		isOnline = false
		for _, OnlineNode = range OnlineNodes {
			if strings.Contains(OnlineNode, Node) {
				Nodes[index] = OnlineNode
				isOnline = true
				break
			}
		}
		if !isOnline {
			Nodes[index] = ""
		}
	}
	return
}

func FileExist(elmName string) bool {
	return len(Locate(elmName)) >= config.GConfig.DataShards
}
