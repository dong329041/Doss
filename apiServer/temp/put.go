package temp

import (
	"apiServer/locate"
	"common"
	"config"
	"io"
	"log"
	"meta"
	"net/http"
	"net/url"
	"stream"
	"utils"
)

// 上传部分临时数据：用于断点续传
// 1) 请求URL：PUT /temp/<object_name>
// 2) 请求头：Range:bytes=<first>-<last>
func put(w http.ResponseWriter, r *http.Request) {
	var (
		putStream   *stream.RSRecoverablePutStream
		getStream   *stream.RSRecoverableGetStream
		currentSize int64
		offset      int64
		putBytes    []byte
		putLen      int
		hashSum     string
		err         error
	)

	// 从token中解析出上传进度，并比对验证客户端上传的offset
	if putStream, err = stream.RecoverPutStream(r); err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if currentSize = putStream.CurrentSize(); currentSize == -1 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if offset = utils.GetOffsetFromHeader(r.Header); currentSize != offset {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	// 读取上传数据：以BlockSize为buffer批次读取并写入RecoverPutStream中
	// NOTE: 除非正好将对象完整上传，否则接口服务每次只接受BlockSize字节的整数倍，不足的部分将被丢弃，
	//       如果客户端的分块小于BlockSize字节，那么上传的数据就会被全部丢弃，
	//       客户端需要在PUT之前可调用temp接口的HEAD方法检查该token当前的进度，并选择合适的偏移量和分块大小
	putBytes = make([]byte, config.GConfig.BlockSize)
	for {
		putLen, err = io.ReadFull(r.Body, putBytes)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println(common.ErrRecoverPut, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if currentSize += int64(putLen); currentSize > putStream.Size {
			putStream.Commit(false)
			log.Println(common.ErrRecoverPutExceedSize)
			w.WriteHeader(http.StatusForbidden)
			return
		}

		// 如果某次读取到的长度不到BlockSize字节且读到的总长度不等于对象的大小，
		// 说明本次客户端上传结束，还有后续数据要上传，此时接口服务丢弃最后那次读到的长度不到BlockSize的数据
		if putLen != config.GConfig.BlockSize && currentSize != putStream.Size {
			return
		}
		putStream.Write(putBytes[:putLen])

		// 如果读到的总长度等于对象的大小，说明客户端上传了全部数据，则执行Flush将剩余数据写入临时对象，
		// 然后生成一个临时对象读取流getStream，读取getStream中的数据并计算hash值；
		// 若散列值一致，则继续检查该散列值是否已经存在，如果存在则删除临时对象，否则将临时对象转正
		if currentSize == putStream.Size {
			putStream.Flush()
			getStream, err = stream.NewRSRecoverableGetStream(putStream.Servers, putStream.UUIDs, putStream.Size)
			if hashSum = url.PathEscape(utils.CalculateHash(getStream)); hashSum != putStream.Hash {
				putStream.Commit(false)
				log.Println(common.ErrRecoverPutHashMismatch)
				w.WriteHeader(http.StatusForbidden)
				return
			}
			if locate.FileExist(url.PathEscape(hashSum)) {
				putStream.Commit(false)
			} else {
				putStream.Commit(true)
			}
			if _, err = meta.NewDossMongo().PutObjectMeta(putStream.Name, putStream.Size, putStream.Hash); err != nil {
				log.Println(common.ErrPutObjectMeta, err)
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		}
	}
}
