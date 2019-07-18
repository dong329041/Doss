package check

import (
	"common"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"common/dataFlag"
	"config"
	"dataServer/locate"
	"meta"
	"meta/funcParams"
	"utils"
)

// 对象在回收站中的持续时间：超过此时间，将被永久删除（单位：秒）
const ObjectGarbageDuration = 10 * 24 * 60 * 60

func ObjectsCheck() {
	var (
		files        []string
		hash         string
		DMongo       *meta.DossMongo
		DMongo2      *meta.DossMongo
		objMeta      *meta.ObjectMeta
		shardMetas   []*meta.ObjectShardMeta
		shardMeta    *meta.ObjectShardMeta
		aggObjNames  []string
		aggObject    *meta.AggObject
		unRefAggObjs []string
		unRefAggObj  string
		hashFiles    []string
		index        int
		fileInfo     os.FileInfo
		srcPath      string
		dstPath      string
		err          error
	)

	utils.CheckFilePath(*dataFlag.StorageRoot + "/garbage")

	// 清除大文件：若最新版本的对象元数据hash值为空字符串，
	// 则说明需要将该对象移除：将对象移到/garbage目录，在之后的定期扫描中清除时间较久的对象
	DMongo = meta.NewDossMongo()
	files, _ = filepath.Glob(*dataFlag.StorageRoot + "/objects/*")
	for index = range files {
		hash = strings.Split(filepath.Base(files[index]), ".")[0]
		if objMeta, err = DMongo.GetMetaByHash(hash); err != nil {
			log.Println(common.ErrGetMetaByHash, err)
			continue
		}
		if objMeta != nil && objMeta.Hash == "" {
			hashFiles, _ = filepath.Glob(*dataFlag.StorageRoot + "/objects/" + hash + ".*")
			if len(hashFiles) != 1 {
				return
			}
			locate.ObjectDelete(hash)
			os.Rename(hashFiles[0], *dataFlag.StorageRoot+"/garbage/"+filepath.Base(hashFiles[0]))
		}
	}

	// 清除对象分片所在的聚合对象
	files, _ = filepath.Glob(*dataFlag.StorageRoot + "/aggregate_objects/*")
	DMongo = meta.NewDossMongo(funcParams.MongoParamCollection(config.GConfig.ObjShardColName))
	DMongo2 = meta.NewDossMongo(funcParams.MongoParamCollection(config.GConfig.AggregateObjColName))
	if shardMetas, err = DMongo.GetShardMetaByHash(""); err != nil {
		log.Println(common.ErrGetShardMetaByHash, err)
		return
	}
	for index = range files {
		aggObjNames = append(aggObjNames, filepath.Base(files[index]))
	}
	// 若该分片所引用的聚合对象位于该数据节点，则将聚合对象的引用数减1，并将该分片元数据删除
	for _, shardMeta = range shardMetas {
		for _, aggObject = range shardMeta.Aggregate {
			if utils.SliceHasMember(aggObjNames, aggObject.Name) {
				_ = DMongo2.UpdateAggregateMeta(aggObject.Name, -1, -1, "")
				_, _ = DMongo.DeleteShardMetaByIndex(shardMeta.Object, shardMeta.Index)
			}
		}
	}
	// 将未被引用的聚合对象放入回收站
	unRefAggObjs, _ = DMongo2.DeleteUnRefAggregates()
	for _, unRefAggObj = range unRefAggObjs {
		locate.AggObjectDelete(unRefAggObj)
		srcPath = *dataFlag.StorageRoot + "/aggregate_objects/" + unRefAggObj
		dstPath = *dataFlag.StorageRoot + "/garbage/" + unRefAggObj
		os.Rename(srcPath, dstPath)
	}

	// 真正移除对象：将回收站中存在时间较久的对象删除
	files, _ = filepath.Glob(*dataFlag.StorageRoot + "/garbage/*")
	for index = range files {
		if fileInfo, err = os.Stat(files[index]); err != nil {
			continue
		}
		if time.Now().Unix()-fileInfo.ModTime().Unix() > ObjectGarbageDuration {
			os.Remove(files[index])
		}
	}
}
