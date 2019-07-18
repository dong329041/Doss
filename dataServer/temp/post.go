package temp

import (
	"common"
	"encoding/json"
	"github.com/satori/go.uuid"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"common/dataFlag"
	"config"
	"dataServer/locate"
	"github.com/mongodb/mongo-go-driver/bson/primitive"
	"meta"
	"meta/funcParams"
	"utils"
)

type tempInfo struct {
	Uuid      string
	Name      string
	Size      int64
	Aggregate []*meta.AggObject
}

func (t *tempInfo) hash() string {
	s := strings.Split(t.Name, ".")
	return s[0]
}

func (t *tempInfo) id() int {
	s := strings.Split(t.Name, ".")
	id, _ := strconv.Atoi(s[1])
	return id
}

func post(w http.ResponseWriter, r *http.Request) {
	var (
		Uuid       uuid.UUID
		UuidStr    string
		name       string
		size       int64
		TempInfo   tempInfo
		aggObjects []*meta.AggObject
		err        error
	)

	// 生成uuid，得出name、size
	Uuid, _ = uuid.NewV4()
	UuidStr = Uuid.String()
	name = strings.Split(r.URL.EscapedPath(), "/")[2]
	if size, err = strconv.ParseInt(r.Header.Get("size"), 0, 64); err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// 执行小文件处理逻辑
	if size < config.GConfig.AggregateObjSize*common.MB {
		if aggObjects, err = postMiniFile(w, name, size); err != nil {
			log.Println("POST:", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	// 生成TempInfo信息并写入文件
	TempInfo = tempInfo{UuidStr, name, size, aggObjects}
	if err = TempInfo.writeToFile(); err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// 向请求端返回uuid字符串
	w.Write([]byte(UuidStr))
}

// ----------------------------------------
// 小文件处理逻辑：获取未满的聚合对象，若不存在未满的聚合对象或者获取到的聚合对象剩余容量不够，则生成新的聚合对象
// 分析：针对多客户端同时并发写，访问同一个聚合对象，则在POST阶段先分配好空间，
//      生成新的聚合对象通过加锁得以保证，更新空间占用通过FindOneAndUpdate来保证操作原子性
// ----------------------------------------
func postMiniFile(w http.ResponseWriter, name string, size int64) (aggObjects []*meta.AggObject, err error) {
	var (
		aggObject      *meta.AggObject
		objectName     string
		shardIndex     int
		shardMeta      *meta.ObjectShardMeta
		aggAvailObjs   []string
		aggAvailObj    string
		totalAvailSize int64
		remainSize     int64
		DMongo         *meta.DossMongo
		index          int
		objectId       primitive.ObjectID
		aggObjMutex    sync.Mutex
	)

	aggObjMutex.Lock()
	defer aggObjMutex.Unlock()

	// 查看数据库中是否有记录，若存在，则说明该请求是由于纠删码修复导致，则不生成新的信息
	DMongo = meta.NewDossMongo(funcParams.MongoParamCollection(config.GConfig.ObjShardColName))
	objectName = strings.Split(name, ".")[0]
	shardIndex, _ = strconv.Atoi(strings.Split(name, ".")[1])
	shardMeta, err = DMongo.GetShardMetaByIndex(objectName, shardIndex)
	if err == nil && len(shardMeta.Aggregate) > 0 {
		aggObjects = shardMeta.Aggregate
		return
	}

	// 获取可用的聚合对象，累加出总的可用空间，并收集到aggObjects数组，用于写入tempInfo文件
	aggAvailObjs = locate.GetAvailAggregates()
	for _, aggAvailObj = range aggAvailObjs {
		totalAvailSize += config.GConfig.AggregateObjSize*common.MB - locate.GetAggObjSize(aggAvailObj)
		aggObject = &meta.AggObject{Name: aggAvailObj, Offset: locate.GetAggObjSize(aggAvailObj)}
		aggObjects = append(aggObjects, aggObject)
	}

	// 若可用空间不足，则再创建一个聚合对象（将聚合对象信息写入数据库、内存中locate信息）
	DMongo = meta.NewDossMongo(funcParams.MongoParamCollection(config.GConfig.AggregateObjColName))
	if len(aggObjects) == 0 || totalAvailSize < size {
		if objectId, err = DMongo.NewAggregateMeta(); err != nil {
			log.Println(common.ErrNewAggMeta, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		locate.UpdateAggObjSize(objectId.Hex(), 0)
		// 将新生成的聚合对象信息收集到aggObjects数组，用于写入tempInfo文件
		aggObject = &meta.AggObject{Name: objectId.Hex(), Offset: 0}
		aggObjects = append(aggObjects, aggObject)
	}

	// 预定数据库中聚合对象的空间（通过FindOneAndUpdate保证原子性）
	// 预定空间只更新数据库，不更新内存中的locate信息，在上传完成后的提交阶段，若成功则更新内存中的聚合对象size信息
	remainSize = size
	for index, aggObject = range aggObjects {
		if len(aggObjects) > 1 && index < len(aggObjects)-1 {
			err = DMongo.UpdateAggregateMeta(
				aggObject.Name, config.GConfig.AggregateObjSize*common.MB, 1, name,
			)
			remainSize -= config.GConfig.AggregateObjSize*common.MB - locate.GetAggObjSize(aggObject.Name)
			aggObject.Size = config.GConfig.AggregateObjSize*common.MB - locate.GetAggObjSize(aggObject.Name)
		} else {
			err = DMongo.UpdateAggregateMeta(aggObject.Name, remainSize, 1, name)
			aggObject.Size = remainSize
		}
		if err != nil {
			log.Println(common.ErrUpdateAggMeta, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
	return
}

// 将temp信息存入文件
func (t *tempInfo) writeToFile() (err error) {
	var (
		file *os.File
		body []byte
	)
	_ = utils.CheckFilePath(*dataFlag.StorageRoot + "/temp")
	if file, err = os.Create(*dataFlag.StorageRoot + "/temp/" + t.Uuid); err != nil {
		return
	}
	defer file.Close()
	body, _ = json.Marshal(t)
	file.Write(body)
	return
}
