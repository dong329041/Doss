package versions

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"meta"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	var (
		method   string
		name     string
		metas    []*meta.ObjectMeta
		resBytes []byte
		i        int
		err      error
	)

	// HTTP Method检查
	if method = r.Method; method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// 查询数据库，获取该对象所有版本的元数据
	name = strings.Split(r.URL.EscapedPath(), "/")[2]
	if metas, err = meta.NewDossMongo().GetAllVersionMetas(name); err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// 返回元数据集合
	for i = range metas {
		resBytes, _ = json.Marshal(metas[i])
		w.Write(resBytes)
		w.Write([]byte("\n"))
	}
}
