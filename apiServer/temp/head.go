package temp

import (
	"fmt"
	"log"
	"net/http"
	"stream"
)

func head(w http.ResponseWriter, r *http.Request) {
	var (
		putStream *stream.RSRecoverablePutStream
		current   int64
		err       error
	)

	// 获取token字符串并生成PUT
	if putStream, err = stream.RecoverPutStream(r); err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	// 获取PUT数据流当前上传的字节数
	if current = putStream.CurrentSize(); current == -1 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// 设置响应头：Content-Length：<token当前上传的字节数>
	w.Header().Set("content-length", fmt.Sprintf("%d", current))
}
