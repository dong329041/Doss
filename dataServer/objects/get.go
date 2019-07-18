package objects

import (
	"crypto/sha256"
	"encoding/base64"
	"hash"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	"common/dataFlag"
	"config"
	"meta"
	"meta/funcParams"
	"utils"
)

func get(w http.ResponseWriter, r *http.Request) {
	var (
		DMongo     *meta.DossMongo
		shardMeta  *meta.ObjectShardMeta
		shardPath  string
		shardName  string
		objectName string
		shardIndex int
		filePath   string
		err        error
	)

	// 参数解析
	shardName = strings.Split(r.URL.EscapedPath(), "/")[2]
	objectName = strings.Split(shardName, ".")[0]
	shardIndex, _ = strconv.Atoi(strings.Split(shardName, ".")[1])

	// 判断分片size，若小于聚合对象最大size，则进行小文件处理逻辑
	DMongo = meta.NewDossMongo(funcParams.MongoParamCollection(config.GConfig.ObjShardColName))
	shardMeta, err = DMongo.GetShardMetaByIndex(objectName, shardIndex)
	if err == nil && len(shardMeta.Aggregate) > 0 {
		getMiniFile(w, shardMeta)
		return
	}

	// 处理大文件逻辑
	shardPath = *dataFlag.StorageRoot + "/objects/" + shardName + ".*"
	filePath = checkFile(shardPath, 0, 0, strings.Split(shardPath, ".")[2])
	if filePath == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	utils.SeekCopy(filePath, w, 0, 0)
}

// 小文件处理逻辑
func getMiniFile(w http.ResponseWriter, shardMeta *meta.ObjectShardMeta) {
	var (
		aggObjects = shardMeta.Aggregate
		aggObject  *meta.AggObject
		aggPath    string
		HashCalc   hash.Hash
		HashSum    string
	)

	// 生成一个哈希计算器，并将该分片所在的聚合对象中对应的数据流式拷贝至该计算器，得出哈希计算结果
	HashCalc = sha256.New()
	for _, aggObject = range aggObjects {
		aggPath = *dataFlag.StorageRoot + "/aggregate_objects/" + aggObject.Name
		utils.SeekCopy(aggPath, HashCalc, int64(aggObject.Offset), int64(aggObject.Size))
	}
	HashSum = url.PathEscape(base64.StdEncoding.EncodeToString(HashCalc.Sum(nil)))

	// 若哈希不一致，则返回404，apiServer会进行该数据流的修复
	// 若哈希一致，则拷贝数据流到http.ResponseWriter
	if HashSum != shardMeta.Hash {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	for _, aggObject = range aggObjects {
		aggPath = *dataFlag.StorageRoot + "/aggregate_objects/" + aggObject.Name
		utils.SeekCopy(aggPath, w, int64(aggObject.Offset), int64(aggObject.Size))
	}
}

// 校验文件数据：若散列值一致则返回文件路径，不一致则返回空路径触发数据修复
func checkFile(path string, offset int64, copyLen int64, expectHash string) (filePath string) {
	var (
		files    []string
		HashCalc hash.Hash
		HashSum  string
	)
	if files, _ = filepath.Glob(path); len(files) != 1 {
		return
	}
	filePath = files[0]

	// 生成一个哈希计算器，并将文件流式拷贝至该计算器，得出哈希计算结果
	HashCalc = sha256.New()
	utils.SeekCopy(filePath, HashCalc, offset, copyLen)
	HashSum = url.PathEscape(base64.StdEncoding.EncodeToString(HashCalc.Sum(nil)))

	// 比对hash值
	if HashSum != expectHash {
		filePath = ""
	}
	return
}
