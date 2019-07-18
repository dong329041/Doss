package locate

import (
	"encoding/json"
	"net/http"
	"strings"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	var (
		locateInfo map[int]string
		elmName    string
		resBytes   []byte
	)
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	elmName = strings.Split(r.URL.EscapedPath(), "/")[2]
	if locateInfo = Locate(elmName); len(locateInfo) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	resBytes, _ = json.Marshal(locateInfo)
	w.Write(resBytes)
}
