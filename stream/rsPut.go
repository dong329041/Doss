package stream

import (
	"common"
	"fmt"
	"io"

	"config"
)

type RSPutStream struct {
	*putEncoder
}

// 创建用于纠删码读写的put数据流
func NewRSPutStream(dataServers []string, hash string, size int64) (stream *RSPutStream, err error) {
	var (
		perShard int64
		writers  []io.Writer
		encoder  *putEncoder
		i        int
	)
	if len(dataServers) != int(config.GConfig.AllShards) {
		err = common.ErrNotEnoughDS
		return
	}

	// 生成纠删码编码器（config.GConfig.AllShards个writer）
	perShard = (size + int64(config.GConfig.DataShards) - 1) / int64(config.GConfig.DataShards)
	writers = make([]io.Writer, config.GConfig.AllShards)
	for i = range writers {
		if dataServers[i] != "" {
			if writers[i], err = NewTempPutStream(
				dataServers[i], fmt.Sprintf("%s.%d", hash, i), perShard,
			); err != nil {
				return
			}
		}
	}
	encoder = NewPutEncoder(writers)

	// 生成纠删码下载流
	stream = &RSPutStream{encoder}
	return
}

// 提交上传数据流
func (s *RSPutStream) Commit(success bool) {
	var i int

	// 将最后一批数据进行Flush（因为数据传输是按照blockSize来分块进行的，所以最后一批数据尚未Flush）
	s.Flush()

	// 提交每个TempPutStream上传流（将io.writer类型断言为TempPutStream类型）
	for i = range s.writers {
		if s.writers[i] != nil {
			s.writers[i].(*TempPutStream).Commit(success)
		}
	}
}
