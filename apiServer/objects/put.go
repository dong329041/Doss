package objects

import (
	"common"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"

	"apiServer/locate"
	"meta"
	"stream"
	"utils"
)

func put(w http.ResponseWriter, r *http.Request) {
	var (
		hash    string
		size    int64
		resCode int
		name    string
		err     error
	)

	// 获取请求头中的hash、size信息
	if hash = utils.GetHashFromHeader(r.Header); hash == "" {
		log.Println(common.ErrMissObjectHash)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	size = utils.GetSizeFromHeader(r.Header)

	// 上传对象
	if resCode, err = putObject(r.Body, hash, size); err != nil {
		log.Println(err)
		w.WriteHeader(resCode)
		return
	}
	if resCode != http.StatusOK {
		w.WriteHeader(resCode)
		return
	}

	// 添加对象元数据（name、size、hash、version）
	name = strings.Split(r.URL.EscapedPath(), "/")[2]
	if _, err = meta.NewDossMongo().PutObjectMeta(name, size, url.PathEscape(hash)); err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// 上传对象并实时验证上传数据的正确性
func putObject(r io.Reader, hash string, size int64) (resCode int, err error) {
	var (
		putStream *stream.RSPutStream
		reader    io.Reader
		calcHash  string
	)

	// 检查当前可访问到的总的分片数量是否>=数据分片数量
	if locate.FileExist(url.PathEscape(hash)) {
		resCode = http.StatusOK
		return
	}

	// 生成对象上传数据流（带有纠删码编码器的数据流）
	if putStream, err = newPutStream(url.PathEscape(hash), size); err != nil {
		resCode = http.StatusInternalServerError
		return
	}

	// 在上传数据流的同时计算对象hash值，io.TeeReader生成的reader会读取r的数据流，
	// 一边送入stream上传数据流中，同时也送入utils包中的CalculateHash函数计算hash值
	// 若hash校验一致，则说明传输过程中没有出错，则调用Commit方法将上传完成的临时对象转正，若不一致则删除临时对象
	reader = io.TeeReader(r, putStream)
	calcHash = utils.CalculateHash(reader)
	if calcHash != url.PathEscape(hash) {
		putStream.Commit(false)
		resCode = http.StatusBadRequest
		err = fmt.Errorf(
			"url pathEscaped object hash mismatch, calculated=%s, requested=%s", calcHash, url.PathEscape(hash),
		)
		return
	}
	putStream.Commit(true)
	resCode = http.StatusOK
	return
}

// 创建上传数据流（经过纠删码编码器处理的流）
func newPutStream(hash string, size int64) (putStream *stream.RSPutStream, err error) {
	var Nodes []string

	// 获取对象定位的数据节点集合
	if Nodes, err = locate.GetLocateNodes(hash); err != nil {
		return
	}
	putStream, err = stream.NewRSPutStream(Nodes, hash, size)
	return
}
