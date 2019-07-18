package hashRing

import (
	"common"
	"context"
	"errors"
	"hash/crc32"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"config"
	"github.com/mongodb/mongo-go-driver/bson/primitive"
	"github.com/mongodb/mongo-go-driver/mongo"
	"meta"
	"meta/funcParams"
	"utils"
)

var (
	GHashRing *HashRing // HashRing结构体单例
)

// 实现sort接口
type uintArray []uint32

func (x uintArray) Len() int           { return len(x) }
func (x uintArray) Less(i, j int) bool { return x[i] < x[j] }
func (x uintArray) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

// HashRing 结构体：
// ring:          哈希环map, key是虚拟cube的hash值，value是该虚拟cube对应的物理节点标识
// sortedRing:    将ring map的key(虚拟cube的hash值)进行排序组成的slice
// members:       当前加入到哈希环中的物理节点, key是物理节点标识，value为true或者false
// weights:       当前环中物理节点的权重，key是物理节点标识, value是该节点权重值
// numberOfCubes: 每个物理节点创建的cube数（权重为1的cube值，若权重为5，则cube数为numberOfCubes * 5
// NOTE: 标识可以是ip，也可以是ip+pid等自定义标识
type HashRing struct {
	ringMap       map[uint32]string
	sortedRing    uintArray
	members       map[string]bool
	weights       map[string]int
	objectIds     map[string]primitive.ObjectID
	numberOfCubes int
	sync.RWMutex
}

// 新建HashRing结构体
func InitHashRing() *HashRing {
	GHashRing = &HashRing{
		ringMap:       make(map[uint32]string),
		members:       make(map[string]bool),
		weights:       make(map[string]int),
		objectIds:     make(map[string]primitive.ObjectID),
		numberOfCubes: config.GConfig.DefaultVirtualCubes,
	}
	return GHashRing
}

// 获取HashRing结构体（若当前没有创建HashRing结构体单例则创建该单例并返回其引用地址）
func GetHashRing() *HashRing {
	if GHashRing != nil {
		return GHashRing
	}
	return InitHashRing()
}

// ----------------------------------
// 持续维护HashRing结构体：监听集合中DS节点变化情况
// NOTICE: 若使用此功能，MongoDB版本需>=3.6，且需使用replicaSet方式部署
// ----------------------------------
func CheckHashRing() {
	var (
		DMongo      *meta.DossMongo
		DMongo2     *meta.DossMongo
		Nodes       []*meta.DsNode
		Node        *meta.DsNode
		CStream     *mongo.ChangeStream
		DNodeChange *meta.DsNode
		empty       bool
		err         error
	)

	// 创建DossMongo操作结构体（DMongo用于操作数据节点信息的表，DMongo2用于操作对象元数据的表）
	DMongo = meta.NewDossMongo(
		funcParams.MongoParamCollection(config.GConfig.NodeColName),
	)
	DMongo2 = meta.NewDossMongo()

	// 先获取所有的数据节点列表
	if Nodes, err = DMongo.GetAllNodes(); err != nil {
		log.Fatal(common.ErrGetAllNode, err)
	}
	for _, Node = range Nodes {
		AddNode(Node.OId, Node.Ip, Node.Weight)
	}

	// 返回*mongo.changeStream，用于监听node表的changeStream
	if CStream, err = DMongo.Collection.Watch(context.TODO(), mongo.Pipeline{}); err != nil {
		log.Fatal(common.ErrNewChangeStream, err)
		return
	}
	defer CStream.Close(context.TODO())

	// 持续监听数据节点表的变化
	// TODO:若meta表不为空，则说明系统中已上传有对象，下一版本需完善数据迁移流程
	for {
		if CStream.Next(context.TODO()) {
			var nodeStream meta.NodeStream
			if err = CStream.Decode(&nodeStream); err != nil {
				continue
			}
			DMongo = meta.NewDossMongo(
				funcParams.MongoParamCollection(config.GConfig.NodeColName),
			)

			switch nodeStream.Type {
			case "insert":
				empty, err = DMongo2.IsMetaCollectionEmpty()
				if err == nil && empty {
					DNodeChange, _ = DMongo.GetNodeByOId(nodeStream.DocKey.ObjectId)
					AddNode(DNodeChange.OId, DNodeChange.Ip, DNodeChange.Weight)
				}
			case "delete":
				empty, err = DMongo2.IsMetaCollectionEmpty()
				if err == nil && empty {
					RemoveNode(GetIpByObjectId(nodeStream.DocKey.ObjectId))
				}
			}
		}
		time.Sleep(time.Second * 1)
	}
}

// 根据objectId获取其物理节点标识
func GetIpByObjectId(oid primitive.ObjectID) (node string) {
	var (
		nodeKey  string
		oidValue primitive.ObjectID
		ring     = GetHashRing()
	)
	for nodeKey, oidValue = range ring.objectIds {
		if oidValue == oid {
			node = nodeKey
			return
		}
	}
	return
}

// 设置当前HashRing结构体的numberOfCubes值
// Notice: SetCubeNumber方法必须在哈希环中没有节点的情况下修改
func SetCubeNumber(num int) (err error) {
	var ring = GetHashRing()

	if len(GHashRing.members) != 0 {
		err = common.ErrForbidSetCubeNum
		return
	}
	if num <= 0 {
		err = common.ErrCubeNumLessThanZero
		return
	}
	ring.numberOfCubes = num
	return
}

// 获取当前哈希环上的所有物理节点
func Members() (nodes []string) {
	var (
		ring = GetHashRing()
		node string
	)

	ring.RLock()
	defer ring.RUnlock()

	for node = range ring.members {
		nodes = append(nodes, node)
	}
	return nodes
}

// 根据物理节点IP和当前cube序号生成key
func generateKey(ip string, i int) string {
	return ip + "#" + strconv.Itoa(i)
}

// 根据上述的key生成hash值
func generateHash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// 添加一个物理节点到哈希环中
// Param:
//   oid:    数据节点表中该物理节点对应的ObjectId
//   node:   物理节点标识（可以是ip，也可以是ip+pid，或其他任何自定义形式）
//   weight: 该物理节点的权重
func AddNode(oid primitive.ObjectID, node string, weight int) {
	var (
		i    int
		ring = GetHashRing()
	)

	ring.Lock()
	defer ring.Unlock()

	if weight <= 0 {
		weight = 1
	}
	for i = 0; i < ring.numberOfCubes*weight; i++ {
		ring.ringMap[generateHash(generateKey(node, i))] = node
	}
	ring.members[node] = true
	ring.weights[node] = weight
	ring.objectIds[node] = oid

	updateSortedRing()
}

// 一次性添加多个节点到哈希环上
// Param:
//   nodeWeight: key是物理节点标识，value是该物理节点的权重值
//   nodeOid:    key是物理节点IP标识，value是该物理节点的mongodb objectId
func AddNodes(nodeWeight map[string]int, nodeOid map[string]primitive.ObjectID) {
	var (
		ring   = GetHashRing()
		node   string
		weight int
		oid    primitive.ObjectID
		i      int
	)

	ring.Lock()
	defer ring.Unlock()

	for node, weight = range nodeWeight {
		if weight <= 0 {
			weight = 1
		}
		for i = 0; i < ring.numberOfCubes*weight; i++ {
			ring.ringMap[generateHash(generateKey(node, i))] = node
		}
		ring.members[node] = true
		ring.weights[node] = weight
	}
	for node, oid = range nodeOid {
		ring.objectIds[node] = oid
	}

	updateSortedRing()
}

// 从哈希环上移除该节点
// param：node：物理节点标识
func RemoveNode(node string) {
	var (
		ring   = GetHashRing()
		weight int
		i      int
	)

	ring.Lock()
	defer ring.Unlock()

	weight = ring.weights[node]
	for i = 0; i < ring.numberOfCubes*weight; i++ {
		delete(ring.ringMap, generateHash(generateKey(node, i)))
	}
	delete(ring.members, node)
	delete(ring.weights, node)
	delete(ring.objectIds, node)
	updateSortedRing()
}

// 获取元素名在哈希环上最接近的物理节点（顺时针）
func GetNode(name string) (node string, err error) {
	var (
		ring  = GetHashRing()
		key   uint32
		index int
	)

	ring.RLock()
	defer ring.RUnlock()

	if len(ring.ringMap) == 0 {
		err = errors.New("empty hash ring")
		return
	}
	key = generateHash(name)
	index = search(key)
	node = ring.ringMap[ring.sortedRing[index]]
	return
}

// GetN方法：获取一个元素定位在N个物理节点上（顺时针查找）
// Param: name：元素名，n：获取的物理节点数量
// NOTE: 若找到的节点数m小于n，则返回这m个节点
func GetNodes(name string, n int) (nodes []string, err error) {
	var (
		ring        = GetHashRing()
		memberCount int
		key         uint32
		i           int
		node        string
		start       int
	)

	ring.RLock()
	defer ring.RUnlock()

	if len(ring.ringMap) == 0 {
		return
	}

	memberCount = len(Members())
	if int64(memberCount) < int64(n) {
		n = int(memberCount)
	}

	// 获取第一个定位的物理节点
	key = generateHash(name)
	i = search(key)
	node = ring.ringMap[ring.sortedRing[i]]
	nodes = append(nodes, node)
	if len(nodes) == n {
		return
	}

	// 顺时针获取其他物理节点
	start = i
	for i = start + 1; i != start; i++ {
		if i >= len(ring.sortedRing) {
			i = 0
		}
		node = ring.ringMap[ring.sortedRing[i]]
		if !utils.SliceHasMember(nodes, node) {
			nodes = append(nodes, node)
		}
		if len(nodes) == n {
			break
		}
	}
	return
}

// 顺时针查找最接近key的hash值的那个cube
func search(key uint32) (index int) {
	var (
		ring        = GetHashRing()
		compareFunc func(x int) bool
	)

	compareFunc = func(x int) bool {
		return ring.sortedRing[x] > key
	}
	index = sort.Search(len(ring.sortedRing), compareFunc)
	if index >= len(ring.sortedRing) {
		index = 0
	}
	return
}

// 更新sortedRing，当哈希环发生变化时，需要更新sortedRing
func updateSortedRing() {
	var (
		ring   = GetHashRing()
		hashes uintArray
		hash   uint32
	)

	for hash = range ring.ringMap {
		hashes = append(hashes, hash)
	}
	sort.Sort(hashes)
	ring.sortedRing = hashes
}
