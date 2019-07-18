package stream

import (
	"github.com/klauspost/reedsolomon"
	"io"

	"config"
)

type putEncoder struct {
	writers []io.Writer
	enc     reedsolomon.Encoder
	buffer  []byte
}

// 生成纠删码上传数据流的编码器
func NewPutEncoder(writers []io.Writer) *putEncoder {
	enc, _ := reedsolomon.New(int(config.GConfig.DataShards), int(config.GConfig.ParityShards))
	return &putEncoder{writers, enc, nil}
}

// 实现io.Writer接口
func (encoder *putEncoder) Write(p []byte) (n int, err error) {
	var (
		remain    int // 剩余写入量
		curOffset int // 当前文件指针位置
		next      int // 下一批读取的量
	)

	remain = len(p)
	curOffset = 0

	// 循环读取，最大读取config.GConfig.BlockSize字节到buffer中，之后执行Flush阶段
	for remain != 0 {
		if next = int(config.GConfig.BlockSize) - len(encoder.buffer); next > remain {
			next = remain
		}
		encoder.buffer = append(encoder.buffer, p[curOffset:curOffset+next]...)
		if len(encoder.buffer) == int(config.GConfig.BlockSize) {
			encoder.Flush()
		}
		curOffset += next
		remain -= next
	}

	n = len(p)
	return
}

// 在Flush阶段，将buffer中的数据进行分片、编码
func (encoder *putEncoder) Flush() {
	var (
		shards [][]byte
		i      int
	)

	if len(encoder.buffer) == 0 {
		return
	}

	// 将buffer中数据分片、编码
	shards, _ = encoder.enc.Split(encoder.buffer)
	encoder.enc.Encode(shards)

	// 将处理好的数据分别送入编码器中对应的io.Writer变量
	for i = range shards {
		if encoder.writers[i] != nil {
			encoder.writers[i].Write(shards[i])
		}
	}

	// 清空buffer，进行下一批数据读取
	encoder.buffer = []byte{}
}
