package locate

import (
	"common"
	"log"
	"net/http"
	"strconv"
	"strings"

	"meta"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	var (
		method string
		name   string
		Meta   *meta.ObjectMeta
		id     int
		err    error
	)

	// HTTP请求检查
	method = r.Method
	if method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// 获取对象元数据
	name = strings.Split(r.URL.EscapedPath(), "/")[2]
	if Meta, err = meta.NewDossMongo().GetLastVersionMeta(name); err != nil {
		log.Println(common.ErrGetLastVersionMeta, err)
	}

	// 根据元数据中的hash值进行定位
	if Meta == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	id = ObjectLocate(Meta.Hash)
	if id != -1 {
		w.Write([]byte(strconv.Itoa(id)))
	}
}
