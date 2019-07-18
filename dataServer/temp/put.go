package temp

import (
	"common"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"

	"common/dataFlag"
	"config"
	"dataServer/locate"
	"meta"
	"meta/funcParams"
	"utils"
)

func put(w http.ResponseWriter, r *http.Request) {
	var (
		uuid        string
		TempInfo    *tempInfo
		infoFile    string
		datFile     string
		file        *os.File
		datFileInfo os.FileInfo
		datFileHash string
		actualSize  int64
		err         error
	)

	// 解析出url中的uuid，据此得出tempInfo文件和.dat临时文件的文件名
	uuid = strings.Split(r.URL.EscapedPath(), "/")[2]
	if TempInfo, err = readFromFile(uuid); err != nil {
		log.Println(common.ErrReadTempInfoFile, err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// 判断文件size，若size小于聚合对象的最大长度，则进行小文件合并处理逻辑
	if TempInfo.Size < config.GConfig.AggregateObjSize*common.MB {
		putMiniFile(w, TempInfo)
		return
	}

	// 打开.dat文件句柄
	infoFile = *dataFlag.StorageRoot + "/temp/" + uuid
	datFile = infoFile + ".dat"
	if file, err = os.Open(datFile); err != nil {
		log.Println(common.ErrOpenTempDatFile, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// 验证size：实际上传的size是否与tempInfo文件中的size属性一致
	if datFileInfo, err = file.Stat(); err != nil {
		file.Close()
		log.Println(common.ErrGetFileStat, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	actualSize = datFileInfo.Size()
	os.Remove(infoFile)
	if actualSize != TempInfo.Size {
		file.Close()
		os.Remove(datFile)
		fmt.Println(common.ErrSizeMismatch, "actual", actualSize, "expect", TempInfo.Size)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// 若通过则重命名该.dat文件为正式文件
	datFileHash = utils.CalculateHash(file)
	file.Close()
	os.Rename(datFile, *dataFlag.StorageRoot+"/objects/"+TempInfo.Name+"."+datFileHash)

	// 将对象信息添加到内存中的locate信息中
	locate.ObjectAdd(TempInfo.hash(), TempInfo.id())
}

// 小文件处理逻辑
func putMiniFile(w http.ResponseWriter, TempInfo *tempInfo) {
	var (
		aggObject      *meta.AggObject
		aggObjPath     string
		file           *os.File
		hashCalculator = sha256.New()
		shardHashSum   string
		DMongo         *meta.DossMongo
		shardMeta      *meta.ObjectShardMeta
		err            error
	)

	// 打开每个聚合对象文件句柄，并将数据属于该分片的数据拷贝到哈希计算器中
	for _, aggObject = range TempInfo.Aggregate {
		aggObjPath = *dataFlag.StorageRoot + "/aggregate_objects/" + aggObject.Name
		if file, err = os.OpenFile(aggObjPath, os.O_RDWR, 0644); err != nil {
			log.Println(common.ErrOpenFile, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		file.Seek(aggObject.Offset, io.SeekStart)
		io.CopyN(hashCalculator, file, aggObject.Size)
		file.Close()
	}

	// 哈希计算器已经收集了所有聚合对象上的数据，计算hash值
	shardHashSum = url.PathEscape(base64.StdEncoding.EncodeToString(hashCalculator.Sum(nil)))

	// 更新信息：内存中聚合对象信息、数据库中的对象分片信息、内存中对象的分片信息
	DMongo = meta.NewDossMongo(funcParams.MongoParamCollection(config.GConfig.ObjShardColName))
	shardMeta, err = DMongo.GetShardMetaByIndex(TempInfo.hash(), TempInfo.id())
	if err == nil && shardMeta.Hash != shardHashSum {
		for _, aggObject = range TempInfo.Aggregate {
			if aggObject.Name != "" {
				locate.UpdateAggObjSize(aggObject.Name, locate.GetAggObjSize(aggObject.Name)+aggObject.Size)
			}
		}
		_, _ = DMongo.PutObjectShardMeta(
			TempInfo.hash(), TempInfo.id(), TempInfo.Size, shardHashSum, TempInfo.Aggregate,
		)
		if TempInfo.hash() != "" {
			locate.ObjectAdd(TempInfo.hash(), TempInfo.id())
		}
	}

	// 移除临时信息文件
	os.Remove(*dataFlag.StorageRoot + "/temp/" + TempInfo.Uuid)
	return
}
