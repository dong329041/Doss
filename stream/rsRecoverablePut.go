package stream

import (
	"common"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"github.com/dgrijalva/jwt-go/request"
	"io"
	"log"
	"net/http"
	"time"

	"config"
	"utils"
)

type recoverableToken struct {
	Name    string
	Size    int64
	Hash    string
	Servers []string
	UUIDs   []string
}

type RSRecoverablePutStream struct {
	*RSPutStream
	*recoverableToken
}

// 生成可恢复的上传数据流（经过纠删码编码器处理的数据流）
func NewRSRecoverablePutStream(dataServers []string, name, hash string, size int64) (
	stream *RSRecoverablePutStream, err error) {

	var (
		putStream *RSPutStream
		uuidSlice []string
		token     *recoverableToken
		i         int
	)

	// 生成纠删码下载流
	if putStream, err = NewRSPutStream(dataServers, hash, size); err != nil {
		return
	}

	// 获取所有TempPutStream中的uuid，并生成用于恢复的token
	uuidSlice = make([]string, config.GConfig.AllShards)
	for i = range uuidSlice {
		if putStream.writers[i] != nil {
			uuidSlice[i] = putStream.writers[i].(*TempPutStream).Uuid
		}
	}
	token = &recoverableToken{name, size, hash, dataServers, uuidSlice}

	// 组合成可恢复的纠删码下载流
	stream = &RSRecoverablePutStream{putStream, token}
	return
}

// 从token中恢复上传数据流（从请求头的Authorization字段解析token，恢复出上传数据流）
func RecoverPutStream(r *http.Request) (stream *RSRecoverablePutStream, err error) {
	var (
		parsedToken *jwt.Token
		mapClaims   jwt.MapClaims
		streamBytes []byte
		streamStr   string
		Token       recoverableToken
		encoder     *putEncoder
		writers     []io.Writer
		i           int
	)

	keyFunc := func(token *jwt.Token) (interface{}, error) {
		return []byte(config.GConfig.JwtSecretKey), nil
	}

	// 从请求头中解析出token并进行验证
	parsedToken, err = request.ParseFromRequest(r, request.AuthorizationHeaderExtractor, keyFunc)
	if err != nil {
		log.Println(common.ErrParseToken, err)
		return
	} else if !parsedToken.Valid {
		return
	}

	// 得出token中的stream信息：
	// NOTE: 1) parsedToken.Claims为jwt.Claims接口变量，需进行类型断言为jwt.MapClaims结构体
	//       2) mapClaims每个字段的value为interface{}类型，也需要进行类型断言得到原有类型
	mapClaims = parsedToken.Claims.(jwt.MapClaims)
	streamStr = mapClaims["stream"].(string)
	if streamBytes, err = base64.StdEncoding.DecodeString(streamStr); err != nil {
		return
	}
	json.Unmarshal(streamBytes, &Token)

	// 构造所有的io.Writer变量：TempPutStream流
	writers = make([]io.Writer, config.GConfig.AllShards)
	for i = range writers {
		if Token.Servers[i] != "" {
			writers[i] = &TempPutStream{Server: Token.Servers[i], Uuid: Token.UUIDs[i]}
		} else {
			writers[i] = nil
		}
	}

	// 构造纠删码编码器
	encoder = NewPutEncoder(writers)
	stream = &RSRecoverablePutStream{
		RSPutStream:      &RSPutStream{encoder},
		recoverableToken: &Token,
	}
	return
}

// 生成加密token：将上传流的信息经过base64编码后用JWT加密生成最终的token
func (s *RSRecoverablePutStream) ToToken() (tokenStr string, err error) {
	var (
		streamBytes []byte
		token       *jwt.Token
		claims      jwt.MapClaims
	)

	// 生成一个JWT (json web token)
	token = jwt.New(jwt.SigningMethodHS256)

	// 填充MapClaims结构体的字段
	// iss: token的发行者；
	// aud: token的客户；
	// exp: 以数字时间定义失效期，也就是10小时以后本token失效；
	// iat: JWT发布时间，能用于决定JWT年龄
	// stream: 经过纠删码编码的可恢复的上传数据流的信息
	streamBytes, _ = json.Marshal(s)
	claims = make(jwt.MapClaims)
	claims["iss"] = "DossApi"
	claims["aud"] = "Doss"
	claims["exp"] = time.Now().Add(time.Hour * time.Duration(10)).Unix()
	claims["iat"] = time.Now().Unix()
	claims["stream"] = base64.StdEncoding.EncodeToString(streamBytes)
	token.Claims = claims

	// 使用密钥生成加密token
	tokenStr, err = token.SignedString([]byte(config.GConfig.JwtSecretKey))
	return
}

// 从token中解析出当前的传输进度
func (s *RSRecoverablePutStream) CurrentSize() (currentSize int64) {
	var (
		response *http.Response
		err      error
	)

	// 请求第一台dataServer的temp接口，获取当前传输进度
	response, err = http.Head(fmt.Sprintf("http://%s/temp/%s", s.Servers[0], s.UUIDs[0]))
	if err != nil {
		log.Println(err)
		currentSize = -1
		return
	}
	if response.StatusCode != http.StatusOK {
		log.Println(response.StatusCode)
		currentSize = -1
		return
	}

	// 从响应头部解析出当前的size
	currentSize = utils.GetSizeFromHeader(response.Header) * int64(config.GConfig.AllShards)
	if int64(currentSize) > s.Size {
		currentSize = s.Size
	}
	return
}
