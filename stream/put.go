package stream

import (
	"common"
	"fmt"
	"io"
	"net/http"
)

// writer指针用于实现Write方法，errCh用于将一个goroutine传输数据的过程中发生的错误传回主协程
type PutStream struct {
	writer *io.PipeWriter
	errCh  chan error
}

// param: server: 服务器地址 (ip:port); object: 对象名
func NewPutStream(server, object string) *PutStream {
	var (
		reader   *io.PipeReader
		writer   *io.PipeWriter
		ch       = make(chan error)
		client   http.Client
		request  *http.Request
		response *http.Response
		err      error
	)

	reader, writer = io.Pipe()

	// 在协程中调用client.Do方法（此方法是阻塞的）（io.Pipe的读写两端必须在不同的协程中）
	// 调用此putStream的Write方法时，reader会读到writer的字节流（管道互联），直至client.Do读到io.EOF
	go func() {
		request, _ = http.NewRequest("PUT", "http://"+server+"/objects/"+object, reader)
		client = http.Client{}
		response, err = client.Do(request)
		if err == nil && response.StatusCode != http.StatusOK {
			err = fmt.Errorf("%s: %d", common.ErrPutStreamResponse.Error(), response.StatusCode)
		}
		ch <- err
	}()

	return &PutStream{
		writer: writer,
		errCh:  ch,
	}
}

// Write方法：实现io.Writer接口
func (w *PutStream) Write(p []byte) (n int, err error) {
	return w.writer.Write(p)
}

// Close方法：
//   1) 关闭writer：为了让管道另一端的reader读到io.EOF，否则在goroutine中运行的client.Do将始终阻塞
//   2) 在数据流关闭时将传输过程中的错误取出
func (w *PutStream) Close() error {
	_ = w.writer.Close()
	return <-w.errCh
}
