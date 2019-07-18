package stream

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

func getHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	if _, err = w.Write([]byte("hello_world")); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func TestGet(t *testing.T) {
	var (
		server    *httptest.Server
		getStream *GetStream
		body      []byte
	)

	server = httptest.NewServer(http.HandlerFunc(getHandler))
	defer server.Close()

	getStream, _ = NewGetStream(server.URL[7:], "test_object")
	body, _ = ioutil.ReadAll(getStream)
	if string(body) != "hello_world" {
		t.Errorf("read body failed, read %s, expect hello_world", body)
	}
}
