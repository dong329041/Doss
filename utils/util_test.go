package utils

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSliceHasMember(t *testing.T) {
	var slice = make([]string, 0)
	slice = append(slice, "hello", "world", "hello", "world", "gopher")
	if !SliceHasMember(slice, "hello") {
		t.Error("SliceHasMember error.")
	}
}

func TestSliceRemoveReplica(t *testing.T) {
	var slice = make([]string, 0)
	slice = append(slice, "hello", "world", "hello", "world", "gopher")
	if len(SliceRemoveReplica(slice)) != 3 {
		t.Error("slice remove replica error:", slice)
	}
}

func TestCalculateHash(t *testing.T) {
	var (
		expect string
		actual string
	)
	expect = "ZI33h+hn+u%2FZIXLAtfIsJUN+cYN7HfJ50HT6QqWGz9s="
	actual = CalculateHash(strings.NewReader("this is object test1 with version 1"))
	if actual != expect {
		t.Errorf("expect %s, but got %s", expect, actual)
	}

	fmt.Println(url.PathEscape(expect))
}

func TestWatchObjects(t *testing.T) {
	var (
		path = "/var/lib/Doss/6/objects"
	)

	// 定义回调函数（目录下的文件发生改变时执行此回调）
	var callbackFunc = func(files []string) {
		fmt.Println(files)
	}

	// 20秒后发送停止监控的信号
	go func() {
		time.Sleep(20 * time.Second)
		log.Println("stop watch.")
		*GetStopWatchSignal() <- true
	}()

	WatchObjects(path, callbackFunc)
}

func TestSeekWrite(t *testing.T) {
	var (
		path    = "/var/lib/Doss/test/test.txt"
		file    *os.File
		written int64
		buffer  []byte
		err     error
	)

	// 创建文件并清空写入
	_ = CheckFilePath(filepath.Dir(path))
	if file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		t.Error("file open error:", err)
	}
	if _, err = file.WriteString("hello world"); err != nil {
		t.Error("file write error, file content length is", err)
	}
	file.Close()

	// 指定offset修改文件内容
	if written, err = SeekWrite(strings.NewReader("gopher"), -1, path, 6); err != nil {
		t.Error(err)
	}
	if written != 6 {
		t.Error("seekWrite error, written:", written)
	}

	// 读取文件，验证是否按预期写入
	buffer = make([]byte, 0)
	if buffer, err = ioutil.ReadFile(path); err != nil {
		t.Error(err)
	}
	if string(buffer) != "hello gopher" {
		t.Error("seekWrite error, file content is \"" + string(buffer) + "\"")
	}
}

func TestTruncateAndSeekWrite(t *testing.T) {
	var (
		path   = "/var/lib/Doss/test/test.txt"
		path2  = "/var/lib/Doss/test/test2.txt"
		path3  = "/var/lib/Doss/test/test3.txt"
		path4  = "/var/lib/Doss/test/test4.txt"
		file   *os.File
		file2  *os.File
		buffer []byte
		err    error
	)

	// 创建文件并清空写入
	_ = CheckFilePath(filepath.Dir(path))
	if file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		t.Error("file open error:", err)
	}
	if _, err = file.WriteString("hello world!!!"); err != nil {
		t.Error("file write error, file content length is", err)
	}

	// 指定offset修改文件内容
	if err = file.Truncate(11); err != nil {
		t.Error(err)
	}
	file.Close()

	// 读取文件，验证是否按预期写入
	if buffer, err = ioutil.ReadFile(path); err != nil {
		t.Error(err)
	}
	if string(buffer) != "hello world" {
		t.Error("Truncate error, file content is \"" + string(buffer) + "\"")
	}

	// 从既定的offset读入文件
	if file, err = os.Open(path); err != nil {
		t.Error("file open error:", err)
	}
	if _, err = file.Seek(6, io.SeekStart); err != nil {
		t.Error(err)
	}
	if buffer, err = ioutil.ReadAll(file); err != nil {
		t.Error(err)
	}
	if string(buffer) != "world" {
		t.Errorf("Got \"%s\", expect \"world\"", string(buffer))
	}
	file.Close()

	// 从既定的offset开始读取，并写入文件
	if file, err = os.OpenFile(path, os.O_RDWR, 0644); err != nil {
		t.Errorf("file %s open error: %s", path, err.Error())
	}
	if file2, err = os.OpenFile(path2, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		t.Errorf("file %s open error: %s", path2, err.Error())
	}
	if _, err = file.Seek(6, io.SeekStart); err != nil {
		t.Error(err)
	}
	io.Copy(file2, file)
	file.Close()
	file2.Close()
	if buffer, err = ioutil.ReadFile(path2); err != nil {
		t.Error(err)
	}
	if string(buffer) != "world" {
		t.Errorf("Got \"%s\", expect \"world\"", string(buffer))
	}

	// 读取文件并写入两个文件
	if file, err = os.OpenFile(path, os.O_RDWR, 0644); err != nil {
		t.Errorf("file %s open error: %s", path, err.Error())
		return
	}
	defer file.Close()
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		t.Error(err)
	}
	SeekWrite(file, 5, path3, 0)
	SeekWrite(file, -1, path4, 0)

	os.Remove(path)
	os.Remove(path2)
	os.Remove(path3)
	os.Remove(path4)
}

func TestSeekCopy(t *testing.T) {
	var (
		srcPath  = "/var/lib/Doss/test/test.txt"
		dstPath  = "/var/lib/Doss/test/test1.txt"
		dstPath2 = "/var/lib/Doss/test/test2.txt"
		dstFile  *os.File
		dstFile2 *os.File
		err      error
	)

	// 创建文件并清空写入
	_ = CheckFilePath(filepath.Dir(dstPath))
	if dstFile, err = os.OpenFile(dstPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		t.Error("file open error:", err)
	}
	defer dstFile.Close()

	if _, err = SeekCopy(srcPath, dstFile, 6, 5); err != nil {
		t.Error(err)
	}

	// 创建文件并清空写入
	_ = CheckFilePath(filepath.Dir(dstPath))
	if dstFile2, err = os.OpenFile(dstPath2, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		t.Error("file open error:", err)
	}
	defer dstFile2.Close()

	if _, err = SeekCopy(srcPath, dstFile2, 6, 0); err != nil {
		t.Error(err)
	}
}

func TestJWT(t *testing.T) {
	// const (
	// 	JwtSecretKey = "welcome to wangshubo's blog"
	// )
	// token := jwt.New(jwt.SigningMethodHS256)
	// claims := make(jwt.MapClaims)
	// claims["exp"] = time.Now().Add(time.Hour * time.Duration(1)).Unix()
	// claims["iat"] = time.Now().Unix()
	// token.Claims = claims
	//
	// tokenString, err := token.SignedString([]byte(JwtSecretKey))
	// if err != nil {
	// 	t.Error(err)
	// }
	// t.Log(tokenString)
	//
	// token, err := request.ParseFromRequest(r, request.AuthorizationHeaderExtractor,
	// 	func(token *jwt.Token) (interface{}, error) {
	// 		return []byte(JwtSecretKey), nil
	// 	})
	//
	// if err == nil {
	// 	if token.Valid {
	// 		next(w, r)
	// 	} else {
	// 		w.WriteHeader(http.StatusUnauthorized)
	// 		fmt.Fprint(w, "Token is not valid")
	// 	}
	// } else {
	// 	w.WriteHeader(http.StatusUnauthorized)
	// 	fmt.Fprint(w, "Unauthorized access to this resource")
	// }
}
