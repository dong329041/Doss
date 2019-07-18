package common

import "errors"

// 全局错误码定义
var (
	// 参数检查、初始化操作的错误码定义
	ErrParseConfig    = errors.New("parse config file error")
	ErrMissObjectSize = errors.New("missing object size in header")
	ErrMissObjectHash = errors.New("missing object hash in header")

	// 数据库操作相关的错误码定义
	ErrNewChangeStream    = errors.New("new ChangeStream failed")
	ErrGetShardMetaByHash = errors.New("get shard meta by hash error")
	ErrGetMetaByHash      = errors.New("get object meta by hash error")
	ErrGetAggInfo         = errors.New("get aggregate object info error")
	ErrUpdateAggMeta      = errors.New("update aggregate meta error")
	ErrNewAggMeta         = errors.New("new aggregate meta error")
	ErrRegisterNode       = errors.New("register dataServer to node collection error")
	ErrGetLastVersionMeta = errors.New("get last version meta error")

	// 文件操作相关的错误码定义
	ErrOpenFile            = errors.New("open file error")
	ErrGetFileStat         = errors.New("get file stat error")
	ErrReadTempInfoFile    = errors.New("read temp info info error")
	ErrOpenTempDatFile     = errors.New("open temp dat file error")
	ErrCopyBodyToFile      = errors.New("copy request body to file error")
	ErrSizeMismatch        = errors.New("copy to file size mismatch")
	ErrWatchFilePath       = errors.New("watch file path error")
	ErrWatcherEvent        = errors.New("get watch file event error")
	ErrCheckPath           = errors.New("check file path error")
	ErrCheckAggObjRootPath = errors.New("check aggregate objects storage root error")
	ErrCheckObjRootPath    = errors.New("check objects storage root error")

	// 一致性哈希相关的错误码定义
	ErrDataLocate          = errors.New("data locate failed")
	ErrNotEnoughDS         = errors.New("cannot find enough dataServer")
	ErrGetAllNode          = errors.New("get all ds nodes error")
	ErrForbidSetCubeNum    = errors.New("nodes already exist in the ring, modify cube number is not allowed")
	ErrCubeNumLessThanZero = errors.New("num must be more than 0, suggest more than 32")

	// 上传下载流相关的错误码定义
	ErrGetStreamUrl           = errors.New("get stream url invalid")
	ErrGetStreamResponse      = errors.New("get stream http response is not ok")
	ErrPutStreamResponse      = errors.New("put stream http response is not ok")
	ErrRecoverPut             = errors.New("recoverable put error")
	ErrRecoverPutExceedSize   = errors.New("recoverable put exceed size")
	ErrRecoverPutHashMismatch = errors.New("recoverable put done but hash mismatch")
	ErrPutObjectMeta          = errors.New("put object meta error")
	ErrWriteToAggObject       = errors.New("write request body to aggregate object file error")

	// 断点续传相关的错误码定义
	ErrOnlySeekCurrent = errors.New("whence only support SeekCurrent")
	ErrOnlyForwardSeek = errors.New("only support forward seek")

	// JWT相关的错误码定义
	ErrNewToken   = errors.New("generate jwt token error")
	ErrParseToken = errors.New("parse jwt token error")
)
