package temp

import (
	"common"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"common/dataFlag"
)

func get(w http.ResponseWriter, r *http.Request) {
	var (
		uuid string
		path string
		file *os.File
		err  error
	)
	// 若访问的是小文件，则在/temp目录下找不到该.dat文件，返回404
	uuid = strings.Split(r.URL.EscapedPath(), "/")[2]
	path = *dataFlag.StorageRoot + "/temp/" + uuid + ".dat"
	if file, err = os.Open(path); err != nil {
		log.Println(common.ErrOpenTempDatFile, err)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	defer file.Close()
	io.Copy(w, file)
}
