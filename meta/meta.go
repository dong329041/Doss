package meta

import (
	"context"
	"sync"

	"github.com/mongodb/mongo-go-driver/bson/primitive"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/options"
	"github.com/mongodb/mongo-go-driver/x/bsonx"
	"meta/funcParams"
)

// -------------------------------------------
// 上传对象元数据（插入一条文档）
// NOTE:
// 	 1) 若name存在则将版本号加1，若不存在则版本号置为1；
// 	 2) 该方法整个执行过程需用原子锁得以并发保证，这是因为：
// 	    可能存在多个协程在GetLastVersionMeta得到都是1，导致真正插入操作InsertOne时插入的版本号全部是2
// -------------------------------------------
// 声明PutObjectMeta操作的原子锁
var putMetaMutex *sync.Mutex

// metaOps包初始化时创建原子锁
func init() {
	putMetaMutex = new(sync.Mutex)
}
func (DMongo *DossMongo) PutObjectMeta(name string, size int64, hash string) (
	insertedID primitive.ObjectID, err error) {

	var (
		version int
		doc     *ObjectMeta
		meta    *ObjectMeta
		result  *mongo.InsertOneResult
	)

	// 将执行过程加原子锁
	putMetaMutex.Lock()
	defer putMetaMutex.Unlock()

	// 获取该对象最新的版本号
	meta, err = DMongo.GetLastVersionMeta(name)
	if err != nil || meta == nil {
		version = 0
	} else {
		version = meta.Version
	}

	// 构造待上传的BSON文档并进行插入
	doc = &ObjectMeta{
		Name:    name,
		Version: version + 1,
		Size:    size,
		Hash:    hash,
	}
	if result, err = DMongo.Collection.InsertOne(context.TODO(), doc); err != nil {
		return
	}

	// result.InsertedID类型为interface{}，故需进行类型断言转换为primitive.ObjectID类型
	insertedID = result.InsertedID.(primitive.ObjectID)
	return
}

// -------------------------------------------
// 获取对象元数据
// call方式：
//   1) GetObjectMeta(name): 返回该对象最新版本的元数据
//   2) GetObjectMeta(name, MetaOptVersion(version)): 返回该对象版本号为version的元数据
// -------------------------------------------
func (DMongo *DossMongo) GetObjectMeta(name string, paramFunc ...funcParams.MetaParamFunc) (
	meta *ObjectMeta, err error) {

	var (
		filter     *NameVersionFilter
		result     *mongo.SingleResult
		version    int
		metaParams *funcParams.MetaParams
	)

	// 获取参数（版本号version，默认值为-1）
	metaParams = funcParams.NewMetaParams(paramFunc)
	version = metaParams.Version

	// 若得出的版本号为-1，则查询最新版本
	if version == -1 {
		return DMongo.GetLastVersionMeta(name)
	}

	// 过滤条件
	filter = &NameVersionFilter{
		Name:    name,
		Version: version,
	}

	// 执行FindOne查询操作（若不存在则将err置为nil）
	if result = DMongo.Collection.FindOne(context.TODO(), filter); result.Err() != nil {
		meta = &ObjectMeta{}
		if err = result.Err(); err == mongo.ErrNoDocuments {
			err = nil
		}
		return
	}
	err = result.Decode(&meta)
	return
}

// -------------------------------------------
// 获取对象最新版本的元数据
// -------------------------------------------
func (DMongo *DossMongo) GetLastVersionMeta(name string) (meta *ObjectMeta, err error) {
	var (
		filter     *NameFilter
		findOption *options.FindOptions
		sortOption *SortMetaByVersion
		cursor     *mongo.Cursor
	)

	// 过滤条件
	filter = &NameFilter{
		Name: name,
	}

	// 设置Find选项（version：倒序，limit：1）
	sortOption = &SortMetaByVersion{
		SortOrder: -1,
	}
	findOption = options.Find().SetSort(sortOption).SetLimit(1)

	// Find最新版本的对象元数据
	if cursor, err = DMongo.Collection.Find(context.TODO(), filter, findOption); err != nil {
		return
	}
	defer cursor.Close(context.TODO())

	// 解码BSON文档
	for cursor.Next(context.TODO()) {
		meta = &ObjectMeta{}
		if err = cursor.Decode(meta); err != nil {
			continue
		}
		break
	}
	return
}

// -------------------------------------------
// 获取对象所有版本的元数据
// -------------------------------------------
func (DMongo *DossMongo) GetAllVersionMetas(name string) (metas []*ObjectMeta, err error) {
	var (
		filter     *NameFilter
		findOption *options.FindOptions
		sortOption *SortMetaByVersion
		cursor     *mongo.Cursor
		meta       *ObjectMeta
	)

	// 过滤条件和排序条件（按照version升序）
	filter = &NameFilter{
		Name: name,
	}
	sortOption = &SortMetaByVersion{
		SortOrder: 1,
	}
	findOption = options.Find().SetSort(sortOption)

	// Find最新版本的对象元数据
	if cursor, err = DMongo.Collection.Find(context.TODO(), filter, findOption); err != nil {
		return
	}
	defer cursor.Close(context.TODO())

	// 解码BSON文档
	for cursor.Next(context.TODO()) {
		meta = &ObjectMeta{}
		if err = cursor.Decode(meta); err != nil {
			continue
		}
		metas = append(metas, meta)
	}
	return
}

// -------------------------------------------
// 根据对象hash值获取对象最新版本的元数据
// -------------------------------------------
func getAllMetasByHash(DMongo *DossMongo, hash string) (metas []*ObjectMeta, err error) {
	var (
		filter     *HashFilter
		findOption *options.FindOptions
		sortOption *SortMetaByVersion
		cursor     *mongo.Cursor
		meta       *ObjectMeta
	)

	// 过滤条件
	filter = &HashFilter{
		Hash: hash,
	}

	// 设置Find选项（version：倒序，limit：1）
	sortOption = &SortMetaByVersion{
		SortOrder: -1,
	}
	findOption = options.Find().SetSort(sortOption).SetLimit(1)

	// Find最新版本的对象元数据
	if cursor, err = DMongo.Collection.Find(context.TODO(), filter, findOption); err != nil {
		return
	}
	defer cursor.Close(context.TODO())

	// 解码BSON文档
	for cursor.Next(context.TODO()) {
		meta = &ObjectMeta{}
		if err = cursor.Decode(meta); err != nil {
			continue
		}
		metas = append(metas, meta)
	}
	return
}

// 按照对象hash获取最新版本的元数据（版本号：逆序）
func (DMongo *DossMongo) GetAllMetasByHash(hash string) (meta []*ObjectMeta, err error) {
	return getAllMetasByHash(DMongo, hash)
}

// 按照对象hash获取最新版本的元数据
func (DMongo *DossMongo) GetMetaByHash(hash string) (meta *ObjectMeta, err error) {
	var metas []*ObjectMeta
	if metas, err = getAllMetasByHash(DMongo, hash); err != nil {
		return
	}
	if metas != nil && len(metas) > 0 {
		meta = metas[0]
	}
	return
}

// -------------------------------------------
// 获取所有的对象版本数量超过count的元数据
// -------------------------------------------
func (DMongo *DossMongo) GetALLTooMuchVersionMeta(count int) (metas []*ObjectMeta, err error) {
	var (
		filter     *VersionFilter
		findOption *options.FindOptions
		cursor     *mongo.Cursor
		meta       *ObjectMeta
	)

	// 过滤条件
	filter = &VersionFilter{
		Version: count + 1,
	}

	// Find最新版本的对象元数据
	if cursor, err = DMongo.Collection.Find(context.TODO(), filter, findOption); err != nil {
		return
	}
	defer cursor.Close(context.TODO())

	// 解码BSON文档
	for cursor.Next(context.TODO()) {
		meta = &ObjectMeta{}
		if err = cursor.Decode(meta); err != nil {
			continue
		}
		metas = append(metas, meta)
	}
	return
}

// -------------------------------------------
// 删除对象元数据
// call方式：
//   1) DeleteObjectMeta(name): 删除该对象所有版本的元数据
//   2) DeleteObjectMeta(name, MetaOptVersion(version)): 删除该对象版本号为version的元数据
// -------------------------------------------
func (DMongo *DossMongo) DeleteObjectMeta(name string, paramFunc ...funcParams.MetaParamFunc) (
	deleteCount int64, err error) {

	var (
		filter     interface{}
		version    int
		metaParams *funcParams.MetaParams
		result     *mongo.DeleteResult
	)

	// 获取选项参数（版本号version，默认值为-1）
	metaParams = funcParams.NewMetaParams(paramFunc)
	version = metaParams.Version

	// 若得出的版本号为-1，则删除所有版本，若不为-1则删除给定版本的元数据
	if version == -1 {
		filter = &NameFilter{
			Name: name,
		}
	} else {
		filter = &NameVersionFilter{
			Name:    name,
			Version: version,
		}
	}

	// 执行删除操作
	if result, err = DMongo.Collection.DeleteMany(context.TODO(), filter); err != nil || result == nil {
		return
	}
	deleteCount = result.DeletedCount
	return
}

// -------------------------------------------
// 查看对象元数据表是否为空
// -------------------------------------------
func (DMongo *DossMongo) IsMetaCollectionEmpty() (empty bool, err error) {
	var count int64

	if count, err = DMongo.Collection.CountDocuments(context.TODO(), &bsonx.Doc{}); err != nil {
		return
	}
	if count == 0 {
		empty = true
	} else {
		empty = false
	}
	return
}

// ===========================================
// 聚合对象元数据操作定义
// ===========================================
// -------------------------------------------
// 生成新的聚合对象元数据
// -------------------------------------------
func (DMongo *DossMongo) NewAggregateMeta() (insertedID primitive.ObjectID, err error) {
	var (
		objectId primitive.ObjectID
		doc      *AggregateMeta
		result   *mongo.InsertOneResult
	)

	// 构造待上传的BSON文档并进行插入
	objectId = primitive.NewObjectID()
	doc = &AggregateMeta{
		ObjectId: objectId,
		Name:     objectId.Hex(),
		Size:     0,
		RefCount: 0,
		RefBy:    []string{},
	}
	if result, err = DMongo.Collection.InsertOne(context.TODO(), doc); err != nil {
		return
	}

	// result.InsertedID类型为interface{}，故需进行类型断言转换为primitive.ObjectID类型
	insertedID = result.InsertedID.(primitive.ObjectID)
	return
}

// -------------------------------------------
// 获取聚合对象元数据
// -------------------------------------------
func (DMongo *DossMongo) GetAggregateMeta(name string) (meta *AggregateMeta, err error) {
	var (
		result *mongo.SingleResult
		filter *AggregateNameFilter
	)

	filter = &AggregateNameFilter{Name: name}
	meta = &AggregateMeta{}
	if result = DMongo.Collection.FindOne(context.TODO(), filter); result.Err() != nil {
		if err = result.Err(); err == mongo.ErrNoDocuments {
			err = nil
		}
		return
	}
	err = result.Decode(&meta)
	return
}

// -------------------------------------------
// 更新聚合对象元数据
// NOTE:
//   1) size: 若size小于0，则不更新size字段
//   2) RefCount: 引用数增加量（比如，增加一个引用则传值为1，减少一个引用则传值为-1）
//   2) RefBy: 增加对象分片引用（若此值为空字符串，则保持不变）
// -------------------------------------------
func (DMongo *DossMongo) UpdateAggregateMeta(name string, size int64, RefCount int, RefBy string) (err error) {
	var (
		filter   *AggregateNameFilter
		update   *AggregateUpdate
		meta     *AggregateMeta
		refCount int
		newRefBy []string
		result   *mongo.SingleResult
	)

	// 过滤条件
	filter = &AggregateNameFilter{
		Name: name,
	}

	// 更新size、ref_count属性
	if meta, err = DMongo.GetAggregateMeta(name); err == nil && meta.Name != "" {
		refCount = RefCount + meta.RefCount
		if RefCount == 1 {
			newRefBy = append(newRefBy, meta.RefBy...)
			newRefBy = append(newRefBy, RefBy)
		} else if RefCount == -1 {
			newRefBy = append(newRefBy, meta.RefBy[:len(RefBy)]...)
		}
	}
	if size < 0 && meta != nil {
		size = meta.Size
	}
	if RefBy == "" && meta != nil {
		newRefBy = meta.RefBy
	}
	update = &AggregateUpdate{
		Set: AggregateSet{Size: size, RefCount: refCount, RefBy: newRefBy},
	}

	// 执行FindOneAndUpdate更新操作（若文档不存在则将err置为nil）
	// NOTE: FindOneAndUpdate可以保证更新操作的原子性
	if result = DMongo.Collection.FindOneAndUpdate(context.TODO(), filter, update); result.Err() != nil {
		if err = result.Err(); err == mongo.ErrNoDocuments {
			err = nil
		}
	}
	return
}

// -------------------------------------------
// 删除聚合对象元数据
// -------------------------------------------
func (DMongo *DossMongo) DeleteAggregateMeta(name string) (deleteCount int64, err error) {
	var (
		filter *AggregateNameFilter
		result *mongo.DeleteResult
	)

	filter = &AggregateNameFilter{
		Name: name,
	}
	if result, err = DMongo.Collection.DeleteMany(context.TODO(), filter); err != nil || result == nil {
		return
	}
	deleteCount = result.DeletedCount
	return
}

// -------------------------------------------
// 删除未被引用的聚合对象元数据（并将该聚合对象的集合收集在names切片中返回）
// -------------------------------------------
func (DMongo *DossMongo) DeleteUnRefAggregates() (names []string, err error) {
	var (
		filter *AggregateRefCountFilter
		cursor *mongo.Cursor
		meta   *AggregateMeta
	)

	filter = &AggregateRefCountFilter{RefCount: 0}
	if cursor, err = DMongo.Collection.Find(context.TODO(), filter); err != nil {
		return
	}
	defer cursor.Close(context.TODO())

	// 解码BSON文档
	for cursor.Next(context.TODO()) {
		meta = &AggregateMeta{}
		if err = cursor.Decode(meta); err != nil {
			continue
		}
		if len(meta.RefBy) > 0 {
			names = append(names, meta.Name)
			_, _ = DMongo.DeleteAggregateMeta(meta.Name)
		}
	}
	return
}

// ===========================================
// 对象分片元数据操作定义
// ===========================================
// -------------------------------------------
// 插入新的对象分片元数据
// -------------------------------------------
func (DMongo *DossMongo) PutObjectShardMeta(object string, index int, size int64, hash string, aggObjects []*AggObject) (
	insertedID primitive.ObjectID, err error) {
	var (
		doc    *ObjectShardMeta
		result *mongo.InsertOneResult
	)

	// 构造待上传的BSON文档并进行插入
	doc = &ObjectShardMeta{
		Object:    object,
		Index:     index,
		Size:      size,
		Hash:      hash,
		Aggregate: aggObjects,
	}
	if result, err = DMongo.Collection.InsertOne(context.TODO(), doc); err != nil {
		return
	}

	// result.InsertedID类型为interface{}，故需进行类型断言转换为primitive.ObjectID类型
	insertedID = result.InsertedID.(primitive.ObjectID)
	return
}

// -------------------------------------------
// 获取对象分片元数据（按照对象名、分片id）
// -------------------------------------------
func (DMongo *DossMongo) GetShardMetaByIndex(object string, index int) (meta *ObjectShardMeta, err error) {
	var (
		result *mongo.SingleResult
		filter *ShardIndexFilter
	)

	filter = &ShardIndexFilter{Object: object, Index: index}
	meta = &ObjectShardMeta{}
	if result = DMongo.Collection.FindOne(context.TODO(), filter); result.Err() != nil {
		if err = result.Err(); err == mongo.ErrNoDocuments {
			err = nil
		}
		return
	}
	err = result.Decode(&meta)
	return
}

// -------------------------------------------
// 获取对象分片元数据（按照对象名、分片id）
// -------------------------------------------
func (DMongo *DossMongo) GetShardMetaByHash(hash string) (metas []*ObjectShardMeta, err error) {
	var (
		filter *ShardHashFilter
		cursor *mongo.Cursor
		meta   *ObjectShardMeta
	)

	// 过滤条件
	filter = &ShardHashFilter{Hash: hash}

	// Find最新版本的对象元数据
	if cursor, err = DMongo.Collection.Find(context.TODO(), filter); err != nil {
		return
	}
	defer cursor.Close(context.TODO())

	// 解码BSON文档
	for cursor.Next(context.TODO()) {
		meta = &ObjectShardMeta{}
		if err = cursor.Decode(meta); err != nil {
			continue
		}
		metas = append(metas, meta)
	}
	return
}

// -------------------------------------------
// 删除对象所有的分片元数据（将所有分片hash标记为空字符串）
// -------------------------------------------
func (DMongo *DossMongo) DeleteShardMetaByObjHash(objHash string) (count int64, err error) {
	var (
		filter *ShardObjectFilter
		update *ShardHashUpdate
		result *mongo.UpdateResult
	)

	filter = &ShardObjectFilter{
		Object: objHash,
	}
	update = &ShardHashUpdate{
		Set: ShardHashFilter{Hash: ""},
	}
	if result, err = DMongo.Collection.UpdateMany(context.TODO(), filter, update); err != nil {
		return
	}
	count = result.ModifiedCount
	return
}

// -------------------------------------------
// 删除对象分片元数据
// -------------------------------------------
func (DMongo *DossMongo) DeleteShardMeta(hash string) (deleteCount int64, err error) {
	var (
		filter *ShardHashFilter
		result *mongo.DeleteResult
	)

	filter = &ShardHashFilter{
		Hash: hash,
	}
	if result, err = DMongo.Collection.DeleteOne(context.TODO(), filter); err != nil || result == nil {
		return
	}
	deleteCount = result.DeletedCount
	return
}
func (DMongo *DossMongo) DeleteShardMetaByIndex(object string, index int) (deleteCount int64, err error) {
	var (
		filter *ShardIndexFilter
		result *mongo.DeleteResult
	)

	filter = &ShardIndexFilter{
		Object: object,
		Index:  index,
	}
	if result, err = DMongo.Collection.DeleteOne(context.TODO(), filter); err != nil || result == nil {
		return
	}
	deleteCount = result.DeletedCount
	return
}

// ===========================================
// 待修复对象分片元数据操作定义
// ===========================================
// -------------------------------------------
// 上传待修复对象分片元数据
// -------------------------------------------
func (DMongo *DossMongo) PutRepairShardMeta(objHash string, shardIndex string, shardHash string) (
	insertedID primitive.ObjectID, err error) {

	var (
		doc    *RepairShard
		result *mongo.InsertOneResult
	)

	// 构造待上传的BSON文档并进行插入
	doc = &RepairShard{
		ObjHash:    objHash,
		ShardIndex: shardIndex,
		ShardHash:  shardHash,
	}
	if result, err = DMongo.Collection.InsertOne(context.TODO(), doc); err != nil {
		return
	}

	// result.InsertedID类型为interface{}，故需进行类型断言转换为primitive.ObjectID类型
	insertedID = result.InsertedID.(primitive.ObjectID)
	return
}

// -------------------------------------------
// 获取待修复对象分片元数据
// -------------------------------------------
func (DMongo *DossMongo) getRepairMetaByFilter(filter interface{}) (shardMeta *RepairShard, err error) {
	var result *mongo.SingleResult
	shardMeta = &RepairShard{}

	if result = DMongo.Collection.FindOne(context.TODO(), filter); result.Err() != nil {
		if err = result.Err(); err == mongo.ErrNoDocuments {
			err = nil
		}
		return
	}
	err = result.Decode(&shardMeta)
	return
}
func (DMongo *DossMongo) GetRepairShardMeta(shardHash string) (shardMeta *RepairShard, err error) {
	return DMongo.getRepairMetaByFilter(&RepairShardFilter{
		ShardHash: shardHash,
	})
}
func (DMongo *DossMongo) GetRepairShardMetaByOId(oid primitive.ObjectID) (shardMeta *RepairShard, err error) {
	return DMongo.getRepairMetaByFilter(&OIdFilter{
		ObjectId: oid,
	})
}
func (DMongo *DossMongo) GetRepairShardMetaByLocker(locker string) (metas []*RepairShard, err error) {
	var (
		filter *RepairLockerFilter
		cursor *mongo.Cursor
		meta   *RepairShard
	)

	// 过滤条件和排序条件（按照version升序）
	filter = &RepairLockerFilter{
		Locker: locker,
	}

	// Find最新版本的对象元数据
	if cursor, err = DMongo.Collection.Find(context.TODO(), filter); err != nil {
		return
	}
	defer cursor.Close(context.TODO())

	// 解码BSON文档
	for cursor.Next(context.TODO()) {
		meta = &RepairShard{}
		if err = cursor.Decode(meta); err != nil {
			continue
		}
		metas = append(metas, meta)
	}
	return
}

// -------------------------------------------
// 获取待修复对象分片元数据
// -------------------------------------------
func (DMongo *DossMongo) UpdateRepairShardMeta(shardHash string, locker string) (err error) {
	var (
		filter *RepairShardFilter
		update *RepairShardUpdate
		result *mongo.SingleResult
	)

	// 过滤条件
	filter = &RepairShardFilter{
		ShardHash: shardHash,
	}

	// 更新locker属性
	update = &RepairShardUpdate{
		Set: RepairShardLocker{Locker: locker},
	}

	// 执行FindOneAndUpdate更新操作（若文档不存在则将err置为nil）
	// NOTE: FindOneAndUpdate可以保证更新操作的原子性
	if result = DMongo.Collection.FindOneAndUpdate(context.TODO(), filter, update); result.Err() != nil {
		if err = result.Err(); err == mongo.ErrNoDocuments {
			err = nil
		}
	}
	return
}

// -------------------------------------------
// 删除待修复对象分片元数据
// -------------------------------------------
func (DMongo *DossMongo) DeleteRepairObjectMeta(shardHash string) (deleteCount int64, err error) {
	var (
		filter *RepairShardFilter
		result *mongo.DeleteResult
	)

	filter = &RepairShardFilter{
		ShardHash: shardHash,
	}
	if result, err = DMongo.Collection.DeleteMany(context.TODO(), filter); err != nil || result == nil {
		return
	}
	deleteCount = result.DeletedCount
	return
}
