package locate

import (
	"common"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"config"
	"meta"
	"meta/funcParams"
	"utils"
)

var objects = make(map[string]int)
var objMutex sync.RWMutex

func GetObjectsInfo() *map[string]int {
	return &objects
}

func ObjectLocate(hash string) int {
	objMutex.RLock()
	id, ok := objects[hash]
	objMutex.RUnlock()
	if !ok {
		return -1
	}
	return id
}

func ObjectAdd(hash string, id int) {
	objMutex.Lock()
	defer objMutex.Unlock()
	objects[hash] = id
}

func ObjectDelete(hash string) {
	objMutex.Lock()
	defer objMutex.Unlock()
	delete(objects, hash)
}

// 收集对象的定位信息
func CollectObjects(storeRoot string) {
	var (
		files    []string
		fileInfo []string
		index    int
		hash     string
		id       int
		err      error
	)

	// 检查存储根目录
	if err = utils.CheckFilePath(storeRoot + "/objects"); err != nil {
		log.Println(common.ErrCheckObjRootPath, err)
		os.Exit(-1)
	}

	// 收集根目录下对象
	files, _ = filepath.Glob(storeRoot + "/objects/*")
	for index = range files {
		fileInfo = strings.Split(filepath.Base(files[index]), ".")
		if len(fileInfo) != 3 {
			log.Panicf(files[index])
		}
		hash = fileInfo[0]
		if id, err = strconv.Atoi(fileInfo[1]); err != nil {
			log.Panicf(err.Error())
		}
		objects[hash] = id
	}

	// 监控根目录对象的变化
	// NOTICE: 此函数调用是一直阻塞的，可用 *utils.GetStopWatchSignal() <- true 结束监听
	utils.WatchObjects(storeRoot+"/objects", repairObjects)
}

// 对象目录下文件发生改变时的回调函数
func repairObjects(changedFiles []string) {
	var (
		DMongo      *meta.DossMongo
		changedFile string
		fileInfo    []string
		objHash     string
		shardIndex  string
		shardHash   string
		shardMeta   *meta.RepairShard
		fileHandler *os.File
		hashSum     string
		err         error
	)

	DMongo = meta.NewDossMongo(funcParams.MongoParamCollection(config.GConfig.RepairObjColName))

	// 修复对象分片数据
	for _, changedFile = range changedFiles {
		fileInfo = strings.Split(filepath.Base(changedFile), ".")
		objHash = fileInfo[0]
		shardIndex = fileInfo[1]
		shardHash = fileInfo[2]

		// 计算hash值
		fileHandler, _ = os.Open(changedFile)
		hashSum = utils.CalculateHash(fileHandler)
		fileHandler.Close()

		// 检查数据库中是否存在该repairObject：
		// 1) 若hash计算结果一致：说明是apiServer修复完成导致到文件变化，则将该条元数据删除；
		// 2) 若hash计算结果不一致：将待修复对象元数据插入到数据库中
		if shardHash == hashSum {
			_, _ = DMongo.DeleteRepairObjectMeta(shardHash)
		} else {
			shardMeta, err = DMongo.GetRepairShardMeta(shardHash)
			if err != nil || shardMeta.ShardHash == "" {
				_, _ = DMongo.PutRepairShardMeta(objHash, shardIndex, shardHash)
			}
		}
	}
}
