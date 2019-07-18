package stream

import (
	"fmt"
	"io"

	"common"
	"config"
)

type RSGetStream struct {
	*getEncoder
}

// 生成纠删码下载流
func NewRSGetStream(locateInfo map[int]string, hash string, size int64) (stream *RSGetStream, err error) {
	var (
		readers   []io.Reader
		reader    io.Reader
		writers   []io.Writer
		shardSize int64
		encoder   *getEncoder
		i         int
	)

	// 获取所有经过dataServer的hash验证后返回的正常读取流
	readers = make([]io.Reader, len(locateInfo))
	for i = 0; i < len(locateInfo); i++ {
		if reader, err = NewGetStream(
			locateInfo[i], fmt.Sprintf("%s.%d", hash, i),
		); err == nil {
			readers[i] = reader
		}
	}

	// 当readers数组中有存在异常读取流nil时，说明此reader对应的分片读取流数据损坏，
	// 那么该reader对应的创建一个分片写入流，用于修复分片，
	// 此时readers数组和writers数组形成互补的关系
	writers = make([]io.Writer, len(locateInfo))
	shardSize = (size + int64(config.GConfig.DataShards) - 1) / int64(config.GConfig.DataShards)
	for i = range readers {
		if readers[i] == nil {
			if writers[i], err = NewTempPutStream(
				locateInfo[i], fmt.Sprintf("%s.%d", hash, i), shardSize,
			); err != nil {
				stream = nil
				return
			}
		}
	}

	// 将readers数组和writers数组生成纠删码的编码器，用于获取正确的数据流
	encoder = NewGetEncoder(readers, writers, size)
	return &RSGetStream{encoder}, nil
}

// 关闭纠删码下载流
// 此时会对需要修复的writer进行commit，即向dataServer发送PUT请求将临时对象rename为正式对象
func (s *RSGetStream) Close() {
	var i int
	// 将io.writer接口变量断言为TempPutStream类型
	for i = range s.writers {
		if s.writers[i] != nil {
			s.writers[i].(*TempPutStream).Commit(true)
		}
	}
}

// 移动纠删码下载流的读取指针（用于断点续传）
func (s *RSGetStream) Seek(offset int64, whence int) (err error) {
	var (
		length int64
		buffer []byte
	)

	// 参数检查：起跳点whence只支持io.SeekCurrent，且只支持向后偏移
	if whence != io.SeekCurrent {
		err = common.ErrOnlySeekCurrent
	}
	if offset < 0 {
		err = common.ErrOnlyForwardSeek
	}

	// 读取offset字节内容并丢弃
	// 调用io.ReadFull读取，每次读取config.GConfig.BlockSize字节到buffer
	for offset != 0 {
		if length = int64(config.GConfig.BlockSize); offset < length {
			length = offset
		}
		buffer = make([]byte, length)
		io.ReadFull(s, buffer)
		offset -= length
	}
	return
}
