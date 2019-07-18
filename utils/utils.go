package utils

import (
	"common"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"log"
	"net/url"
	"os"
	"strings"
)

// 判断slice中是否存在member
func SliceHasMember(slice []string, member string) bool {
	for _, m := range slice {
		if m == member {
			return true
		}
	}
	return false
}

// slice元素去重
func SliceRemoveReplica(slice []string) (outSlice []string) {
	var (
		member   string
		ok       bool
		sliceMap = make(map[string]bool)
	)
	for _, member = range slice {
		if _, ok = sliceMap[member]; !ok {
			outSlice = append(outSlice, member)
			sliceMap[member] = true
		}
	}
	return
}

// 判断slice中是否包含member
// NOTE: 若slice中某元素为"test_hello"，member为"hello"，则返回该下标值
func SliceIndexOfMember(slice []string, member string) int {
	for i, m := range slice {
		if strings.Contains(m, member) {
			return i
		}
	}
	return -1
}

// io.Reader计算sha256散列值
func CalculateHash(r io.Reader) string {
	var Hash = sha256.New()
	io.Copy(Hash, r)
	return url.PathEscape(base64.StdEncoding.EncodeToString(Hash.Sum(nil)))
}

// 判断目录是否存在并递归创建目录
func CheckFilePath(path string) (err error) {
	if _, err = os.Stat(path); err == nil {
		return
	}
	if !os.IsNotExist(err) {
		log.Println(common.ErrCheckPath, path)
		os.Exit(2)
	} else {
		err = os.MkdirAll(path, os.ModePerm)
	}
	return
}

// 从既定的offset写入文件（接收参数为io.reader）
// 参数说明：
//   reader：源reader；
//   readLen：从源reader读取的长度（若该值小于0，则全部读取并拷贝到目的文件）；
//   path：目的文件路径；
//   offset：打开目的文件句柄后文件指针的偏移量
func SeekWrite(reader io.Reader, readLen int64, path string, offset int64) (written int64, err error) {
	// 打开文件（若不存在则创建）
	var file *os.File
	if file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return
	}
	defer file.Close()

	// 设置文件指针并拷贝数据流
	if _, err = file.Seek(offset, io.SeekCurrent); err != nil {
		return
	}
	if readLen > 0 {
		written, err = io.CopyN(file, reader, readLen)
	} else {
		written, err = io.Copy(file, reader)
	}

	return
}

// 从既定的offset读入文件
// 参数说明：
//   filePath：源文件完整路径；
//   writer：目的地io.writer；
//   offset：打开源文件句柄后文件指针的偏移量；
//   copyLen：从源文件拷贝的字节数（NOTICE：若该值不大于0，则全部拷贝）
func SeekCopy(filePath string, writer io.Writer, offset int64, copyLen int64) (written int64, err error) {
	var file *os.File
	if file, err = os.Open(filePath); err != nil {
		return
	}
	defer file.Close()

	// 设置文件指针偏移量，拷贝数据
	if _, err = file.Seek(offset, io.SeekCurrent); err != nil {
		return
	}
	if copyLen > 0 {
		written, err = io.CopyN(writer, file, copyLen)
	} else {
		written, err = io.Copy(writer, file)
	}

	return
}
