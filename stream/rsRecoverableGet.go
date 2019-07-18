package stream

import (
	"io"

	"config"
)

type RSRecoverableGetStream struct {
	*getEncoder
}

// 生成可恢复的下载数据流
func NewRSRecoverableGetStream(dataServers []string, UUIDs []string, size int64) (
	stream *RSRecoverableGetStream, err error) {
	var (
		readers []io.Reader
		writers []io.Writer
		encoder *getEncoder
		i       int
	)

	// 生成纠删码编码器（同样数量的reader和writer，形成互补的关系）
	readers = make([]io.Reader, config.GConfig.AllShards)
	for i = 0; i < config.GConfig.AllShards; i++ {
		if readers[i], err = NewTempGetStream(dataServers[i], UUIDs[i]); err != nil {
			return
		}
	}
	writers = make([]io.Writer, config.GConfig.AllShards)
	encoder = NewGetEncoder(readers, writers, size)

	// 生成可恢复的下载流（经过纠删码编码的）
	stream = &RSRecoverableGetStream{encoder}
	return
}
