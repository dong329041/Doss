package stream

import (
	"github.com/klauspost/reedsolomon"
	"io"

	"config"
)

type getEncoder struct {
	readers    []io.Reader
	writers    []io.Writer
	enc        reedsolomon.Encoder
	size       int64
	buffer     []byte
	bufferSize int
	total      int64
}

// 生成纠删码读取数据流的编码器
func NewGetEncoder(readers []io.Reader, writers []io.Writer, size int64) *getEncoder {
	enc, _ := reedsolomon.New(int(config.GConfig.DataShards), int(config.GConfig.ParityShards))
	return &getEncoder{
		readers,
		writers,
		enc,
		size,
		nil,
		0,
		0,
	}
}

// 为下载流编码器实现io.Reader接口
func (encoder *getEncoder) Read(p []byte) (n int, err error) {

	// 调用readData读取数据，此时得到的数据为修复后正确的数据
	if encoder.bufferSize == 0 {
		err = encoder.readData()
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return
		}
	}
	n = len(p)
	if encoder.bufferSize < n {
		n = encoder.bufferSize
	}
	encoder.bufferSize -= n

	// 将数据拷贝到p，进行下一批数据的读取
	copy(p, encoder.buffer[:n])
	encoder.buffer = encoder.buffer[n:]
	return
}

// 读取数据：将数据读取到buffer中，在buffer中进行分片处理后，将正确的数据返回给Read()
func (encoder *getEncoder) readData() (err error) {
	var (
		shards     [][]byte
		repairIds  []int
		shardSize  int64
		needRepair bool
		i          int
		n          int
	)

	// 若读取到的数据长度等于size，表明读取完成，返回io.EOF标志
	if encoder.total == encoder.size {
		return io.EOF
	}

	// 预处理readers数组：
	// 1) reader不为nil：将config.GConfig.BlockPerShard读入内存buffer，作为后面修复的源数据
	//    纠删码的Reconstruct修复因为在内存中计算，故需开辟buffer，一批一批数据进行修复
	// 2) reader为nil：将下标收集到repairIds数组中
	shards = make([][]byte, config.GConfig.AllShards)
	repairIds = make([]int, 0)
	needRepair = false
	for i = range encoder.readers {
		if encoder.readers[i] == nil {
			repairIds = append(repairIds, i)
			needRepair = true
		} else {
			shards[i] = make([]byte, config.GConfig.BlockPerShard)
			n, err = io.ReadFull(encoder.readers[i], shards[i])
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				shards[i] = nil
			} else if n != config.GConfig.BlockPerShard {
				shards[i] = shards[i][:n]
			}
		}
	}

	// 修复出正确的数据返回给调用者，并将需要修复写入的分片写入此temp数据流
	if needRepair {
		if err = encoder.enc.Reconstruct(shards); err != nil {
			return
		}
		for i = range repairIds {
			encoder.writers[repairIds[i]].Write(shards[repairIds[i]])
		}
	}

	// 累加编码器中的cache、cacheSize、total值
	for i = 0; i < int(config.GConfig.DataShards); i++ {
		shardSize = int64(len(shards[i]))
		if encoder.total+shardSize > encoder.size {
			shardSize -= encoder.total + shardSize - encoder.size
		}
		encoder.buffer = append(encoder.buffer, shards[i][:shardSize]...)
		encoder.bufferSize += int(shardSize)
		encoder.total += shardSize
	}
	return
}
