package temp

import (
	"common"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"common/dataFlag"
	"config"
	"dataServer/locate"
	"meta"
	"utils"
)

func patch(w http.ResponseWriter, r *http.Request) {
	var (
		uuid        string
		TempInfo    *tempInfo
		infoFile    string
		datFile     string
		file        *os.File
		datFileInfo os.FileInfo
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
	infoFile = *dataFlag.StorageRoot + "/temp/" + uuid

	// 判断文件size，若size小于聚合对象的最大长度，则进行小文件合并处理逻辑
	if TempInfo.Size < config.GConfig.AggregateObjSize*common.MB {
		patchMiniFile(w, r, TempInfo)
		return
	}

	// 创建.dat文件并打开文件句柄
	datFile = infoFile + ".dat"
	if file, err = os.OpenFile(datFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644); err != nil {
		log.Println(common.ErrOpenTempDatFile, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// 将request body内容流式拷贝到.dat文件中
	if _, err = io.Copy(file, r.Body); err != nil {
		log.Println(common.ErrCopyBodyToFile, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// 验证实际上传的size是否与tempInfo文件中的size属性一致
	// 若不一致，则移除tempInfo文件和.dat临时文件
	if datFileInfo, err = file.Stat(); err != nil {
		log.Println(common.ErrGetFileStat, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if actualSize = datFileInfo.Size(); actualSize > TempInfo.Size {
		os.Remove(datFile)
		os.Remove(infoFile)
		log.Println(common.ErrSizeMismatch, "actual", actualSize, "expect", TempInfo.Size)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// 根据uuid文件名解析tempInfo文件
func readFromFile(uuid string) (info *tempInfo, err error) {
	var (
		file      *os.File
		fileBytes []byte
	)
	if file, err = os.Open(*dataFlag.StorageRoot + "/temp/" + uuid); err != nil {
		return
	}
	defer file.Close()
	fileBytes, _ = ioutil.ReadAll(file)
	json.Unmarshal(fileBytes, &info)
	return
}

// 小文件处理逻辑
func patchMiniFile(w http.ResponseWriter, r *http.Request, TempInfo *tempInfo) {
	var (
		aggObjects []*meta.AggObject
		aggObject  *meta.AggObject
		path       string
		err        error
	)

	// 检查aggregate_objects目录
	_ = utils.CheckFilePath(*dataFlag.StorageRoot + "/aggregate_objects")

	// 获取该分片数据占据的聚合对象信息（tempInfo信息）
	if aggObjects = TempInfo.Aggregate; len(aggObjects) == 0 {
		log.Println(common.ErrGetAggInfo, locate.GetAggObjectsInfo())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// 将上传流写入聚合对象文件
	for _, aggObject = range aggObjects {
		path = *dataFlag.StorageRoot + "/aggregate_objects/" + aggObject.Name
		if _, err = utils.SeekWrite(r.Body, aggObject.Size, path, aggObject.Offset); err != nil {
			log.Println(common.ErrWriteToAggObject, err)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}

	return
}
