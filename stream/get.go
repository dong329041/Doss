package stream

import (
	"common"
	"fmt"
	"io"
	"net/http"
)

type GetStream struct {
	reader io.Reader
}

// 创建对象读取流
// param: server: 服务器地址 (ip:port); object: 对象名（若为纠删码下载流，则object应为：对象名.分片下标）
func NewGetStream(server, object string) (getStream *GetStream, err error) {
	if server == "" || object == "" {
		err = common.ErrGetStreamUrl
		return
	}
	getStream, err = newGetStream("http://" + server + "/objects/" + object)
	return
}

func newGetStream(url string) (stream *GetStream, err error) {
	var response *http.Response
	if response, err = http.Get(url); err != nil {
		return
	}
	if response.StatusCode != http.StatusOK {
		stream = nil
		err = fmt.Errorf("%s: %d", common.ErrGetStreamResponse.Error(), response.StatusCode)
		return
	}
	stream = &GetStream{response.Body}
	return
}

// 实现io.Read接口
func (r *GetStream) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}
