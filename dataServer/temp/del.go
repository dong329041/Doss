package temp

import (
	"common"
	"log"
	"net/http"
	"os"
	"strings"

	"common/dataFlag"
	"config"
	"dataServer/locate"
	"meta"
	"meta/funcParams"
)

func del(w http.ResponseWriter, r *http.Request) {
	var (
		uuid     string
		infoFile string
		datFile  string
		TempInfo *tempInfo
		err      error
	)

	uuid = strings.Split(r.URL.EscapedPath(), "/")[2]
	if TempInfo, err = readFromFile(uuid); err != nil {
		log.Println(common.ErrReadTempInfoFile, err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// 判断文件size，若size小于聚合对象最大size，则进行小文件处理逻辑
	if TempInfo.Size < config.GConfig.AggregateObjSize*common.MB {
		deleteMiniFile(w, TempInfo)
		return
	}

	infoFile = *dataFlag.StorageRoot + "/temp/" + uuid
	datFile = infoFile + ".dat"
	os.Remove(infoFile)
	os.Remove(datFile)
}

// 小文件处理逻辑
func deleteMiniFile(w http.ResponseWriter, TempInfo *tempInfo) {
	var (
		aggObject  *meta.AggObject
		aggObjFile string
		DMongo     *meta.DossMongo
		aggMeta    *meta.AggregateMeta
		file       *os.File
		err        error
	)

	// 打开每个聚合对象文件句柄，并回滚到上传之前的size
	DMongo = meta.NewDossMongo(funcParams.MongoParamCollection(config.GConfig.AggregateObjColName))
	for _, aggObject = range TempInfo.Aggregate {
		aggObjFile = *dataFlag.StorageRoot + "/aggregate_objects/" + aggObject.Name
		if file, err = os.OpenFile(aggObjFile, os.O_RDWR, 0644); err != nil {
			log.Println(common.ErrOpenFile, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		// 如果在上传数据期间没有另外的数据占据后面的空间，则回滚到该对象上传之前的size以及聚合对象元数据信息
		if aggMeta, err = DMongo.GetAggregateMeta(aggObject.Name); err == nil && aggMeta.Name != "" {
			if aggMeta.Size == aggObject.Offset+aggObject.Size {
				file.Truncate(aggObject.Offset)
				err = DMongo.UpdateAggregateMeta(aggObject.Name, aggObject.Offset, -1, "")
				if err != nil {
					log.Println(common.ErrUpdateAggMeta, err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
			}
		}
		locate.AggObjectDelete(aggObject.Name)
	}

	os.Remove(*dataFlag.StorageRoot + "/temp/" + TempInfo.Uuid)
	return
}
