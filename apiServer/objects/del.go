package objects

import (
	"config"
	"meta"
	"meta/funcParams"

	"log"
	"net/http"
	"strings"
)

func del(w http.ResponseWriter, r *http.Request) {
	var (
		DMongo  *meta.DossMongo
		objMeta *meta.ObjectMeta
		objName string
		err     error
	)

	// 删除对象时，只是将元数据中该对象的hash设置为空字符串（此为删除标记的约定）
	DMongo = meta.NewDossMongo()
	objName = strings.Split(r.URL.EscapedPath(), "/")[2]
	objMeta, err = DMongo.GetObjectMeta(objName)
	if _, err = DMongo.PutObjectMeta(objName, 0, ""); err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// 若该对象为小文件，则将其分片的hash也标记为空字符串
	DMongo = meta.NewDossMongo(funcParams.MongoParamCollection(config.GConfig.ObjShardColName))
	_, _ = DMongo.DeleteShardMetaByObjHash(objMeta.Hash)
}
