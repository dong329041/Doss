package utils

import (
	"net/http"
	"strconv"
	"strings"
)

// 从请求头部解析出对象offset
func GetOffsetFromHeader(header http.Header) (offset int64) {
	var (
		byteRange string
		bytePos   []string
	)

	offset = 0
	if byteRange = header.Get("range"); len(byteRange) < 7 || byteRange[:6] != "bytes=" {
		return
	}
	bytePos = strings.Split(byteRange[6:], "-")
	offset, _ = strconv.ParseInt(bytePos[0], 0, 64)
	return
}

// 从请求头部解析出对象hash值
func GetHashFromHeader(header http.Header) (hash string) {
	var digest string

	if digest = header.Get("digest"); len(digest) < 9 || digest[:8] != "SHA-256=" {
		hash = ""
		return
	}
	hash = digest[8:]
	return
}

// 从请求头部解析出对象size值
func GetSizeFromHeader(header http.Header) (size int64) {
	size, _ = strconv.ParseInt(header.Get("content-length"), 0, 64)
	return
}
