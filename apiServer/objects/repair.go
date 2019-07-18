package objects

import (
	"common"
	"context"
	"io"
	"log"
	"strconv"
	"time"

	"apiServer/heartbeat"
	"common/apiFlag"
	"config"
	"github.com/mongodb/mongo-go-driver/mongo"
	"meta"
	"meta/funcParams"
	"stream"
	"utils"
)

// 监听对象损坏并进行修复
func ListenObjectsRepair() {
	var (
		DMongo       *meta.DossMongo
		CStream      *mongo.ChangeStream
		repairStream meta.RepairStream
		repairMeta   *meta.RepairShard
		err          error
	)

	DMongo = meta.NewDossMongo(
		funcParams.MongoParamCollection(config.GConfig.RepairObjColName),
	)
	if CStream, err = DMongo.Collection.Watch(context.TODO(), mongo.Pipeline{}); err != nil {
		log.Fatal(common.ErrNewChangeStream, err)
		return
	}
	defer CStream.Close(context.TODO())

	// 检查是否有上次宕机前未修复成功的任务（属于自己的任务）
	checkSelfRepairJob()

	// 持续监听待修复对象分片元数据表的变化
	for {
		if CStream.Next(context.TODO()) {
			if err = CStream.Decode(&repairStream); err != nil {
				continue
			}

			// 若改变类型不为insert，则跳过此次修复
			if repairStream.Type != "insert" {
				continue
			}

			// 获取发生改变的document
			DMongo = meta.NewDossMongo(
				funcParams.MongoParamCollection(config.GConfig.RepairObjColName),
			)
			repairMeta, err = DMongo.GetRepairShardMetaByOId(repairStream.DocKey.ObjectId)
			if err != nil || repairMeta.ShardHash == "" {
				continue
			}

			// 抢分布式乐观锁
			// 1) 若Locker属性不为空，说明被其他apiServer捷足先登了，则跳过此次修复；
			// 2) 若Locker为空，说明当前修复任务未被其他apiServer抢占，则自己加锁抢占，并启动修复
			if repairMeta.Locker != "" {
				continue
			} else {
				err = DMongo.UpdateRepairShardMeta(
					repairMeta.ShardHash,
					*apiFlag.ListenIp+":"+strconv.Itoa(*apiFlag.ListenPort),
				)
				// 如果抢锁失败，说明多个apiServer同时在抢且被别人抢到了，则跳过此次修复
				if err != nil {
					continue
				}
				go repairObject(repairMeta.ObjHash)
			}
		}
		time.Sleep(time.Second * 1)
	}
}

// 该函数不对外提供，限制由apiServer的objects包来进行修复
func repairObject(objHash string) {
	var (
		Meta      *meta.ObjectMeta
		getStream *stream.RSGetStream
		err       error
	)

	// 请求对象元数据
	if Meta, err = meta.NewDossMongo().GetMetaByHash(objHash); err != nil || Meta.Name == "" {
		return
	}

	// 生成对象下载流
	if getStream, err = GetStream(Meta); err != nil {
		return
	}

	// 读取分片数据（读取过程中会进行修复），读取到的数据丢到生成的黑洞io.Writer中
	var writer = utils.NewNullWriter()
	io.Copy(writer, getStream)

	// 调用Close方法将GetStream中分片修复的数据流提交转正，dataServer将临时对象转为正式对象
	getStream.Close()
}

// 检查是否有上次宕机前未修复成功的任务（属于自己的任务）
func checkSelfRepairJob() {
	var (
		DMongo     *meta.DossMongo
		locker     string
		shardMetas []*meta.RepairShard
		shardMeta  *meta.RepairShard
		err        error
	)

	// 收到数据节点心跳后才开始进行修复工作
	for {
		if len(heartbeat.GetOnlineDataServers()) > 0 {
			time.Sleep(config.GConfig.HeartbeatInterval * time.Second)
			break
		}
		time.Sleep(config.GConfig.HeartbeatInterval * time.Second)
	}

	// 检查数据表中是否存在locker设置为自己的待修复对象（即上次宕机前未完成的任务）
	DMongo = meta.NewDossMongo(funcParams.MongoParamCollection(config.GConfig.RepairObjColName))
	locker = *apiFlag.ListenIp + ":" + strconv.Itoa(*apiFlag.ListenPort)
	shardMetas, err = DMongo.GetRepairShardMetaByLocker(locker)
	if err != nil {
		return
	}

	// 每个任务起一个协程修复
	for _, shardMeta = range shardMetas {
		go repairObject(shardMeta.ObjHash)
	}
}
