package stream

import (
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

func putHandler(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		err  error
	)

	if body, err = ioutil.ReadAll(r.Body); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
	if string(body) != "test_object_content" {
		w.WriteHeader(http.StatusForbidden)
	}
}

func TestPut(t *testing.T) {
	var (
		server    *httptest.Server
		putStream *PutStream
		err       error
	)

	server = httptest.NewServer(http.HandlerFunc(putHandler))
	defer server.Close()

	putStream = NewPutStream(server.URL[7:], "test_object")
	_, _ = io.WriteString(putStream, "test_object_content")

	// 捕捉传输过程中发生的错误
	if err = putStream.Close(); err != nil {
		t.Error(err)
	}
}
