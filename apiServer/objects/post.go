package objects

import (
	"common"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"apiServer/locate"
	"meta"
	"stream"
	"utils"
)

// POST方法：用于创建token
func post(w http.ResponseWriter, r *http.Request) {
	var (
		name      string
		size      int64
		hash      string
		nodes     []string
		putStream *stream.RSRecoverablePutStream
		tokenStr  string
		err       error
	)

	// 获取对象name、size、hash
	name = strings.Split(r.URL.EscapedPath(), "/")[2]
	if size, err = strconv.ParseInt(r.Header.Get("size"), 0, 64); err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(common.ErrMissObjectSize.Error()))
		return
	}
	if hash = utils.GetHashFromHeader(r.Header); hash == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(common.ErrMissObjectHash.Error()))
		return
	}

	// 如果该散列值已经存在，则直接往元数据服务addVersion并返回200 OK；
	if locate.FileExist(url.PathEscape(hash)) {
		_, err = meta.NewDossMongo().PutObjectMeta(name, size, url.PathEscape(hash))
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		return
	}

	// 获取数据定位节点，并生成经过纠删码编码的可恢复的上传数据流，用于将数据断点续传
	if nodes, err = locate.GetLocateNodes(url.PathEscape(hash)); err != nil {
		log.Println(common.ErrDataLocate, err)
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	putStream, err = stream.NewRSRecoverablePutStream(nodes, name, url.PathEscape(hash), size)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// 在"location"响应头中附加token返回给客户端，并返回201 Created
	// 客户端拿到后可以直接向该URI发送PUT请求，将数据上传
	if tokenStr, err = putStream.ToToken(); err != nil {
		log.Println(common.ErrNewToken, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("location", "/temp/"+url.PathEscape(tokenStr))
	w.WriteHeader(http.StatusCreated)
}
