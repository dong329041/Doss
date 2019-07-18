package locate

import (
	"common"
	"crypto/sha256"
	"encoding/base64"
	"hash"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"config"
	"meta"
	"meta/funcParams"
	"utils"
)

var aggObjects = make(map[string]int64) // key: 聚合对象名；value：聚合对象当前尺寸
var aggObjMutex sync.RWMutex

// 实现聚合对象数组的排序接口（按照size排序从大到小）
type AggObjSlice []string

func (obj AggObjSlice) Len() int { return len(obj) }

func (obj AggObjSlice) Swap(i, j int) { obj[i], obj[j] = obj[j], obj[i] }

func (obj AggObjSlice) Less(i, j int) bool { return GetAggObjSize(obj[i]) > GetAggObjSize(obj[j]) }

// 获取全部聚合对象信息
func GetAggObjectsInfo() *map[string]int64 {
	return &aggObjects
}

// 获取所有可用的聚合对象（按照剩余空间从大到小排序）
func GetAvailAggregates() (aggObjs AggObjSlice) {
	var (
		name string
		size int64
	)
	for name, size = range aggObjects {
		if size < config.GConfig.AggregateObjSize*common.MB {
			aggObjs = append(aggObjs, name)
		}
	}
	sort.Sort(aggObjs)
	return
}

// 删除指定的聚合对象信息
func AggObjectDelete(name string) {
	aggObjMutex.Lock()
	defer aggObjMutex.Unlock()
	delete(aggObjects, name)
}

// 获取聚合对象的size
func GetAggObjSize(name string) (size int64) {
	var ok bool
	aggObjMutex.RLock()
	size, ok = aggObjects[name]
	aggObjMutex.RUnlock()
	if !ok {
		return 0
	}
	return
}

// 更新聚合对象的size（若不存在则创建）
func UpdateAggObjSize(name string, newSize int64) {
	aggObjMutex.Lock()
	defer aggObjMutex.Unlock()
	aggObjects[name] = newSize
}

// 收集聚合对象的定位信息
func CollectAggObjects(storeRoot string) {
	var (
		files       []string
		file        string
		fileHandler *os.File
		fileInfo    os.FileInfo
		aggName     string
		err         error
	)

	// 检查存储根目录
	if err = utils.CheckFilePath(storeRoot + "/aggregate_objects"); err != nil {
		log.Println(common.ErrCheckAggObjRootPath, err)
		os.Exit(-1)
	}

	// 收集根目录下对象
	files, _ = filepath.Glob(storeRoot + "/aggregate_objects/*")
	for _, file = range files {
		if fileHandler, err = os.Open(file); err != nil {
			log.Println(common.ErrOpenFile, err)
			continue
		}
		if fileHandler == nil {
			continue
		}
		if fileInfo, err = fileHandler.Stat(); err != nil {
			fileHandler.Close()
			log.Println(common.ErrGetFileStat, err)
			continue
		}
		fileHandler.Close()
		aggName = filepath.Base(file)
		aggObjects[aggName] = fileInfo.Size()
	}

	// 监控根目录对象的变化
	// NOTICE: 此函数调用是一直阻塞的，可用 *utils.GetStopWatchSignal() <- true 结束监听
	utils.WatchObjects(storeRoot+"/aggregate_objects", repairAggObjects)
}

// 聚合对象目录下文件发生改变时的回调函数
func repairAggObjects(changedFiles []string) {
	var (
		DMongoAgg    *meta.DossMongo
		DMongoRepair *meta.DossMongo
		DMongoShard  *meta.DossMongo
		changedFile  string
		aggMeta      *meta.AggregateMeta
		shardMeta    *meta.ObjectShardMeta
		refShard     string
		objectName   string
		shardIndex   int
		repairMeta   *meta.RepairShard
		HashCalc     hash.Hash
		aggObject    *meta.AggObject
		HashSum      string
		err          error
	)

	DMongoAgg = meta.NewDossMongo(funcParams.MongoParamCollection(config.GConfig.AggregateObjColName))
	DMongoRepair = meta.NewDossMongo(funcParams.MongoParamCollection(config.GConfig.RepairObjColName))
	DMongoShard = meta.NewDossMongo(funcParams.MongoParamCollection(config.GConfig.ObjShardColName))

	// 修复对象分片数据
	for _, changedFile = range changedFiles {
		// 得到聚合对象的引用信息，若数据库中聚合对象size不等于内存中的聚合对象缓存信息，
		// 则说明当前聚合对象上有正常的上传数据流，则不应修复聚合对象
		aggMeta, err = DMongoAgg.GetAggregateMeta(filepath.Base(changedFile))
		if err != nil || aggMeta.RefCount == 0 {
			continue
		}
		if aggMeta.Size != GetAggObjSize(filepath.Base(changedFile)) {
			continue
		}

		for _, refShard = range aggMeta.RefBy {
			objectName = strings.Split(refShard, ".")[0]
			shardIndex, _ = strconv.Atoi(strings.Split(refShard, ".")[1])
			shardMeta, err = DMongoShard.GetShardMetaByIndex(objectName, shardIndex)
			if err != nil || shardMeta.Hash == "" {
				continue
			}

			// 生成一个哈希计算器，并将该分片所在的聚合对象中对应的数据流式拷贝至该计算器，得出哈希计算结果
			HashCalc = sha256.New()
			for _, aggObject = range shardMeta.Aggregate {
				utils.SeekCopy(changedFile, HashCalc, int64(aggObject.Offset), int64(aggObject.Size))
			}
			HashSum = url.PathEscape(base64.StdEncoding.EncodeToString(HashCalc.Sum(nil)))

			// 检查数据库中是否存在该repairObject：
			// 1) 若hash计算结果一致：说明是apiServer修复完成导致到文件变化，则将该条元数据删除；
			// 2) 若hash计算结果不一致：将待修复对象元数据插入到数据库中
			if shardMeta.Hash == HashSum {
				_, _ = DMongoRepair.DeleteRepairObjectMeta(shardMeta.Hash)
			} else {
				repairMeta, err = DMongoRepair.GetRepairShardMeta(shardMeta.Hash)
				if err != nil || repairMeta.ShardHash == "" {
					_, _ = DMongoRepair.PutRepairShardMeta(objectName, strconv.Itoa(shardIndex), shardMeta.Hash)
				}
			}
		}
	}
}
