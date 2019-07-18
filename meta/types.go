package meta

import (
	"context"
	"time"

	"config"
	"github.com/mongodb/mongo-go-driver/bson/primitive"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
	"meta/funcParams"
	"utils"
)

type OIdFilter struct {
	ObjectId primitive.ObjectID `bson:"_id"`
}

// ================================
// 系统对象元数据类型定义
// ================================
type ObjectMeta struct {
	Name    string `bson:"name"`    // 对象名
	Version int    `bson:"version"` // 对象版本号
	Size    int64  `bson:"size"`    // 对象大小
	Hash    string `bson:"hash"`    // 对象hash值
}

type NameVersionFilter struct {
	Name    string `bson:"name"`
	Version int    `bson:"version"`
}

type NameFilter struct {
	Name string `bson:"name"`
}

type VersionFilter struct {
	Version int `bson:"version"`
}

type HashFilter struct {
	Hash string `bson:"hash"`
}

type SortMetaByVersion struct {
	SortOrder int `bson:"version"`
}

// 关于MongoDB操作的结构体
type DossMongo struct {
	Database   *mongo.Database
	Collection *mongo.Collection
}

// ---------------------------------
// 创建DossMongo结构体
// NOTE：
//   1) 若使用types.go中默认的数据库名、集合名，则直接调用NewDossMongo()
//   2) 若获取自定义的集合，则可调用如下形式：
//          NewDossMeta(OptionDatabase("dbName"))
//          NewDossMeta(OptionCollection("colName"))
//          NewDossMeta(OptionDatabase("dbName"), OptionCollection("colName"))
//   3) 若只创建mongo.Database变量，则调用NewMongoClient().Database("dbName")
// ---------------------------------
func NewDossMongo(optionFunctions ...funcParams.MongoParamFunc) *DossMongo {
	var (
		Options *funcParams.MongoParams
		client  *mongo.Client
		ctx     context.Context
		err     error
	)

	// 生成默认参数
	Options = funcParams.NewMongoParams(optionFunctions)

	// 生成Mongo结构体
	ctx, _ = context.WithTimeout(context.Background(), config.GConfig.MongoConnectTimeout*time.Second)
	client, err = mongo.Connect(ctx, options.Client().ApplyURI(utils.GetMongodbUrl()))
	if err != nil {
		panic(err)
	}
	return &DossMongo{
		Database:   client.Database(Options.DatabaseName),
		Collection: client.Database(Options.DatabaseName).Collection(Options.CollectionName),
	}
}

// ================================
// 数据节点元数据类型定义
// ================================
type DsNode struct {
	OId    primitive.ObjectID `bson:"_id"`    // objectID
	Ip     string             `bson:"ip"`     // 节点ip
	Weight int                `bson:"weight"` // 节点权重
}

type NodeIpFilter struct {
	Ip string `bson:"ip"`
}

// 关于node表changeStream的定义
type NodeStream struct {
	Type   string    `bson:"operationType"`
	DocKey OIdFilter `bson:"documentKey"`
}

// ================================
// 聚合对象元数据类型定义
// ================================
type AggregateMeta struct {
	ObjectId primitive.ObjectID `bson:"_id"`       // 聚合对象id（聚合对象名）
	Name     string             `bson:"name"`      // 聚合对象名（生成的MongoDB objectId字符串）
	Size     int64              `bson:"size"`      // 当前聚合对象已经存入多少字节的数据
	RefCount int                `bson:"ref_count"` // 当前聚合对象被多少小对象引用
	RefBy    []string           `bson:"ref_by"`    // 当前聚合对象被哪些小对象引用
}

type AggregateNameFilter struct {
	Name string `bson:"name"`
}

type AggregateRefCountFilter struct {
	RefCount int `bson:"ref_count"`
}

type AggregateUpdate struct {
	Set AggregateSet `bson:"$set"`
}

type AggregateSet struct {
	Size     int64    `bson:"size"`
	RefCount int      `bson:"ref_count"`
	RefBy    []string `bson:"ref_by"`
}

// 用于写入tempInfo文件的分片信息所占据的聚合对象结构体信息
// Name：聚合对象名；Offset：该分片在此聚合对象上的offset；Size：该分片在聚合对象上占据的size
type AggObject struct {
	Name   string `json:"name"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
}

// ================================
// 对象分片元数据类型定义
// ================================
type ObjectShardMeta struct {
	Object    string       `bson:"object"`    // 分片所属的对象名
	Index     int          `bson:"index"`     // 分片index
	Size      int64        `bson:"size"`      // 分片数据的size值
	Hash      string       `bson:"hash"`      // 分片数据的hash值
	Aggregate []*AggObject `bson:"aggregate"` // 分片的数据位于哪几个集合对象上
}

type ShardObjectFilter struct {
	Object string `bson:"object"`
}

type ShardIndexFilter struct {
	Object string `bson:"object"`
	Index  int    `bson:"index"`
}

type ShardHashUpdate struct {
	Set ShardHashFilter `bson:"$set"`
}

type ShardHashFilter struct {
	Hash string `bson:"hash"`
}

// ================================
// 待修复对象分片元数据类型定义
// ================================
type RepairShard struct {
	ObjHash    string `bson:"objHash"`
	ShardIndex string `bson:"shardIndex"`
	ShardHash  string `bson:"shardHash"`
	Locker     string `bson:"locker"`
}

type RepairShardFilter struct {
	ShardHash string `bson:"shardHash"`
}

type RepairLockerFilter struct {
	Locker string `bson:"locker"`
}

type RepairShardUpdate struct {
	Set RepairShardLocker `bson:"$set"`
}

type RepairShardLocker struct {
	Locker string `bson:"locker"`
}

// 关于待修复对象分片元数据表changeStream的定义
type RepairStream struct {
	Type   string    `bson:"operationType"`
	DocKey OIdFilter `bson:"documentKey"`
}
