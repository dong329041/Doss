package stream

import (
	"common"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

type TempPutStream struct {
	Server string
	Uuid   string
}

// 生成temp上传流：调用dataServer的temp接口进行hash验证，
// 若dataServer验证hash通过，则返回数据流，否则返回404，由apiServer生成修复数据流
func NewTempGetStream(server, uuid string) (*GetStream, error) {
	return newGetStream("http://" + server + "/temp/" + uuid)
}

func NewTempPutStream(server, object string, size int64) (putStream *TempPutStream, err error) {
	var (
		url      string
		request  *http.Request
		client   http.Client
		response *http.Response
		resUuid  []byte
	)
	url = "http://" + server + "/temp/" + object
	if request, err = http.NewRequest("POST", url, nil); err != nil {
		return
	}
	request.Header.Set("size", fmt.Sprintf("%d", size))
	client = http.Client{}
	if response, err = client.Do(request); err != nil {
		return
	}
	if resUuid, err = ioutil.ReadAll(response.Body); err != nil {
		return
	}
	putStream = &TempPutStream{server, string(resUuid)}
	return
}

// 实现io.Writer接口：向dataServer的temp接口发送PATCH请求
// 此处约定为PATCH方法的原因在于：dataServer对于小文件的处理是合并到聚合对象中，所以需用PATCH，而不是PUT
func (w *TempPutStream) Write(p []byte) (n int, err error) {
	var (
		url      string
		request  *http.Request
		client   http.Client
		response *http.Response
	)
	url = "http://" + w.Server + "/temp/" + w.Uuid
	if request, err = http.NewRequest("PATCH", url, strings.NewReader(string(p))); err != nil {
		return
	}
	client = http.Client{}
	if response, err = client.Do(request); err != nil {
		return
	}
	if response.StatusCode != http.StatusOK {
		err = fmt.Errorf("%s: %d", common.ErrPutStreamResponse.Error(), response.StatusCode)
		return
	}
	n = len(p)
	return
}

// 提交temp数据流：
// 若参数为true，则向dataServer的temp接口发送PUT请求，用于将临时文件转正；
// 若参数为false，则向dataServer的temp接口发送DELETE请求，用于将临时文件删除。
func (w *TempPutStream) Commit(success bool) {
	var (
		method  string
		request *http.Request
		client  http.Client
	)
	method = "DELETE"
	if success {
		method = "PUT"
	}
	request, _ = http.NewRequest(method, "http://"+w.Server+"/temp/"+w.Uuid, nil)
	client = http.Client{}
	client.Do(request)
}
