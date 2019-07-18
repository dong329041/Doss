package utils

import "config"

// 实现一个黑洞io.Writer
type NullWriter struct {
	Buffer []byte
}

func NewNullWriter() *NullWriter {
	var buffer []byte
	return &NullWriter{
		Buffer: buffer,
	}
}

// 实现io.Writer接口（将数据一批一批读取到内存中并丢弃）
func (writer *NullWriter) Write(p []byte) (n int, err error) {
	var (
		remain    int // 剩余读取量
		next      int // 下一批读取的量
		curOffset int // 当前读取指针位置
	)

	remain = len(p)
	curOffset = 0

	// 循环读取，最大读取config.GConfig.BlockSize字节到buffer中，并在Flush阶段，将数据从内存中丢弃
	for remain != 0 {
		if next = int(config.GConfig.BlockSize) - len(writer.Buffer); next > remain {
			next = remain
		}
		writer.Buffer = append(writer.Buffer, p[curOffset:curOffset+next]...)
		if len(writer.Buffer) == int(config.GConfig.BlockSize) {
			writer.Flush()
		}
		curOffset += next
		remain -= next
	}
	n = len(p)

	// 清空最后的缓存
	writer.Flush()
	return
}

// 在Flush阶段，将数据从内存中丢弃
func (writer *NullWriter) Flush() {
	writer.Buffer = []byte{}
}
