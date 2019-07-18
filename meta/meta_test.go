package meta

import (
	"context"
	"sync"
	"testing"

	"config"
	"github.com/mongodb/mongo-go-driver/bson/primitive"
	"github.com/mongodb/mongo-go-driver/x/bsonx"
	"meta/funcParams"
)

func TestDossMongo_PutObjectMeta(t *testing.T) {
	var (
		DMongo     *DossMongo
		insertedID primitive.ObjectID
		err        error
	)

	DMongo = NewDossMongo()
	_ = DMongo.Collection.Drop(context.TODO())

	if insertedID, err = DMongo.PutObjectMeta("test", 1024, "hash_value_test"); err != nil {
		t.Error(err)
	}
	if insertedID.Hex() == "" {
		t.Error("Put object'id:", insertedID.Hex())
	}

	// 将表drop，恢复环境
	_ = DMongo.Collection.Drop(context.TODO())
}

// 测试高并发场景下上传对象元数据的正确性
func TestDossMongo_PutObjectMeta_Concurrent(t *testing.T) {
	var (
		DMongo   *DossMongo
		meta     *ObjectMeta
		metas    []*ObjectMeta
		err      error
		index    int
		goIndex  int
		goNumber = 1000
	)

	DMongo = NewDossMongo()
	_ = DMongo.Collection.Drop(context.TODO())

	// 并发上传对象元数据
	wg := new(sync.WaitGroup)
	for goIndex = 0; goIndex < goNumber; goIndex++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = DMongo.PutObjectMeta("test", 1024, "hash_value_test")
		}()
	}
	wg.Wait()

	// 检查并发插入的对象元数据是否正确
	if metas, err = DMongo.GetAllVersionMetas("test"); err != nil {
		t.Error(err)
	}
	if len(metas) != goNumber {
		t.Errorf("Got meta numbers is %d, expect: %d", len(metas), goNumber)
	}
	for index, meta = range metas {
		if index != meta.Version-1 {
			t.Errorf("Got meta version is %d, expect: %d", meta.Version, index)
		}
	}

	// 将表drop，恢复环境
	_ = DMongo.Collection.Drop(context.TODO())
}

func TestDossMongo_GetObjectMeta(t *testing.T) {
	var (
		DMongo *DossMongo
		meta   *ObjectMeta
		err    error
	)

	DMongo = NewDossMongo()
	_ = DMongo.Collection.Drop(context.TODO())

	// 生成两个版本
	_, _ = DMongo.PutObjectMeta("test", 1024, "hash_value_test")
	_, _ = DMongo.PutObjectMeta("test", 1024, "hash_value_test")

	// 获取最新版本的元数据
	meta, err = DMongo.GetObjectMeta("test")
	if err != nil {
		t.Error(err)
	}
	if meta == nil {
		t.Error("Got meta:", meta)
	}
	if meta.Version != 2 {
		t.Error("Got meta version:", meta.Version, "expect: 2")
	}

	// 获取给定版本的元数据
	if meta, err = DMongo.GetObjectMeta("test", funcParams.MetaParamVersion(1)); err != nil {
		t.Error(err)
	}
	if meta == nil {
		t.Error("Got meta:", meta)
	}
	if meta.Version != 1 {
		t.Error("Got meta version:", meta.Version, "expect: 1")
	}

	// 将表drop，恢复环境
	_ = DMongo.Collection.Drop(context.TODO())
}

func TestDossMongo_GetAllVersionMetas(t *testing.T) {
	var (
		DMongo *DossMongo
		metas  []*ObjectMeta
		err    error
	)

	DMongo = NewDossMongo()
	_ = DMongo.Collection.Drop(context.TODO())

	// 生成两个版本
	_, _ = DMongo.PutObjectMeta("test", 1024, "hash_value_test")
	_, _ = DMongo.PutObjectMeta("test", 1024, "hash_value_test")

	if metas, err = DMongo.GetAllVersionMetas("test"); err != nil {
		t.Error(err)
	}
	if len(metas) != 2 {
		t.Error("Got meta numbers is", len(metas), "expect: 2")
	}

	// 将表drop，恢复环境
	_ = DMongo.Collection.Drop(context.TODO())
}

func TestDossMongo_GetMetaByHash(t *testing.T) {
	var (
		DMongo *DossMongo
		meta   *ObjectMeta
		err    error
	)

	DMongo = NewDossMongo(funcParams.MongoParamCollection("object_test"))
	_ = DMongo.Collection.Drop(context.TODO())

	// 生成两个版本
	_, _ = DMongo.PutObjectMeta("test", 1024, "hash_value_test")
	_, _ = DMongo.PutObjectMeta("test", 1024, "hash_value_test")

	if meta, err = DMongo.GetMetaByHash("hash_value_test"); err != nil {
		t.Error(err)
	}
	if meta.Version != 2 {
		t.Error("Get last version meta by hash failed, got:", meta)
	}

	// 将表drop，恢复环境
	_ = DMongo.Collection.Drop(context.TODO())
}

func TestDossMongo_GetALLTooMuchVersionMeta(t *testing.T) {
	var (
		DMongo *DossMongo
		metas  []*ObjectMeta
		meta   *ObjectMeta
		err    error
	)

	DMongo = NewDossMongo(funcParams.MongoParamCollection("object_test"))
	_ = DMongo.Collection.Drop(context.TODO())

	for i := 0; i < 5; i++ {
		_, _ = DMongo.PutObjectMeta("test", 35, "gI6PB7nGboZQ0+m642uF1lBhxy7OLBIy+7jMZZ2zh2U=")
		_, _ = DMongo.PutObjectMeta("test2", 35, "ZI33h+hn+u%2FZIXLAtfIsJUN+cYN7HfJ50HT6QqWGz9s=")
	}
	_, _ = DMongo.PutObjectMeta("test3", 35, "6nNluthEXVxf5+AKT%2Fs88+a5oysuwqKyGuFi6DGc8PA=")

	if metas, err = DMongo.GetALLTooMuchVersionMeta(4); err != nil {
		t.Error(err)
	}
	for _, meta = range metas {
		t.Log(meta)
	}
	if len(metas) != 2 {
		t.Error("Get meta count failed, got count:", len(metas))
	}

	// 将表drop，恢复环境
	_ = DMongo.Collection.Drop(context.TODO())
}

func TestDossMongo_DeleteObjectMeta(t *testing.T) {
	var (
		DMongo      *DossMongo
		deleteCount int64
		err         error
	)

	DMongo = NewDossMongo()
	_ = DMongo.Collection.Drop(context.TODO())

	// 生成三个版本
	_, _ = DMongo.PutObjectMeta("test", 1024, "hash_value_test")
	_, _ = DMongo.PutObjectMeta("test", 1024, "hash_value_test")
	_, _ = DMongo.PutObjectMeta("test", 1024, "hash_value_test")

	// 删除版本号为1的元数据
	if deleteCount, err = DMongo.DeleteObjectMeta("test", funcParams.MetaParamVersion(1)); err != nil {
		t.Error(err)
	}
	if deleteCount != 1 {
		t.Error("Deleted", deleteCount, "meta, expect: 1")
	}

	// 删除剩余的两条元数据
	if deleteCount, err = DMongo.DeleteObjectMeta("test"); err != nil {
		t.Error(err)
	}
	if deleteCount != 2 {
		t.Error("Deleted", deleteCount, "meta, expect: 2")
	}

	// 将表drop，恢复环境
	_ = DMongo.Collection.Drop(context.TODO())
}

// 测试NewDossMongo的可选参数
func TestNewDossMongo(t *testing.T) {
	var (
		DMongo *DossMongo
	)

	DMongo = NewDossMongo()
	if DMongo.Database.Name() != config.GConfig.DatabaseName {
		t.Error("DatabaseName: expected", config.GConfig.DatabaseName, ", but got", DMongo.Database.Name())
	}
	if DMongo.Collection.Name() != config.GConfig.ObjectColName {
		t.Error("CollectionName: expected", config.GConfig.ObjectColName, ", but got", DMongo.Collection.Name())
	}
}

func TestNewDossMongo2(t *testing.T) {
	var (
		DMongo *DossMongo
	)

	DMongo = NewDossMongo(
		funcParams.MongoParamDatabase("local"),
		funcParams.MongoParamCollection("startup_log"),
	)
	if DMongo.Database.Name() != "local" {
		t.Error("DatabaseName: expected local, but got", DMongo.Database.Name())
	}
	if DMongo.Collection.Name() != "startup_log" {
		t.Error("CollectionName: expected startup_log, but got", DMongo.Collection.Name())
	}
}

func TestDossMongo_IsMetaCollectionEmpty(t *testing.T) {
	var (
		DMongo *DossMongo
		empty  bool
		err    error
	)

	DMongo = NewDossMongo()
	if _, err = DMongo.PutObjectMeta("test", 1024, "hash_value_test"); err != nil {
		t.Error("PutObjectMeta failed:", err)
		return
	}
	if empty, err = DMongo.IsMetaCollectionEmpty(); err != nil {
		t.Error("Execute countDocument failed:", err)
		return
	}
	if empty {
		t.Error("Get meta collection is empty, expect not empty")
	}

	_ = DMongo.Collection.Drop(context.TODO())
	if empty, err = DMongo.IsMetaCollectionEmpty(); err != nil {
		t.Error("Execute countDocument failed:", err)
		return
	}
	if !empty {
		t.Error("Get meta collection is not empty, expect empty")
	}
}

// -------------------------------
// 测试聚合对象元数据的操作
// -------------------------------
func TestDossMongo_NewAggregateMeta(t *testing.T) {
	var (
		DMongo     *DossMongo
		insertedID primitive.ObjectID
		err        error
	)

	DMongo = NewDossMongo(funcParams.MongoParamCollection(config.GConfig.AggregateObjColName))
	_ = DMongo.Collection.Drop(context.TODO())

	if insertedID, err = DMongo.NewAggregateMeta(); err != nil {
		t.Error("PutRepairShardMeta failed:", err)
		return
	}
	if insertedID.Hex() == "" {
		t.Error("PutRepairShardMeta id:", insertedID.Hex())
	}

	// 将表drop，恢复环境
	_ = DMongo.Collection.Drop(context.TODO())
}

func TestDossMongo_UpdateAggregateMetaAndDelete(t *testing.T) {
	var (
		DMongo      *DossMongo
		objectId    primitive.ObjectID
		name        string
		meta        *AggregateMeta
		deleteCount int64
		err         error
	)

	DMongo = NewDossMongo(funcParams.MongoParamCollection(config.GConfig.AggregateObjColName))
	_ = DMongo.Collection.Drop(context.TODO())

	if objectId, err = DMongo.NewAggregateMeta(); err != nil {
		t.Error("PutRepairShardMeta failed:", err)
		return
	}
	if objectId.Hex() == "" {
		t.Error("PutRepairShardMeta id:", objectId.Hex())
	}

	// 更新文档
	name = objectId.Hex()
	err = DMongo.UpdateAggregateMeta(name, 16, 1, "test_ref_by")
	if err != nil {
		t.Error(err)
	}

	// 获取更新后的文档
	if meta, err = DMongo.GetAggregateMeta(name); err != nil {
		t.Error(err)
	}
	if meta.Size != 16 || meta.RefCount != 1 {
		t.Error("update meta failed, meta:", meta)
	}

	// 删除元数据
	if deleteCount, err = DMongo.DeleteAggregateMeta(name); err != nil {
		t.Error(err)
	}
	if deleteCount != 1 {
		t.Error("delete meta failed, deleteCount:", deleteCount)
	}
	_ = DMongo.Collection.Drop(context.TODO())
}

// -------------------------------
// 测试对象分片元数据的操作
// -------------------------------
func TestDossMongo_PutObjectShardMetaAndDelete(t *testing.T) {
	var (
		DMongo      *DossMongo
		shardMeta   *ObjectShardMeta
		objectId    primitive.ObjectID
		deleteCount int64
		aggregate   []*AggObject
		err         error
	)

	DMongo = NewDossMongo(funcParams.MongoParamCollection(config.GConfig.ObjShardColName))
	_ = DMongo.Collection.Drop(context.TODO())

	aggregate = append(aggregate, &AggObject{"test_aggregate_name", 0, 16})
	objectId, err = DMongo.PutObjectShardMeta("test_object", 0, 16, "test_shard_hash", aggregate)
	if err != nil {
		t.Error("PutObjectShardMeta failed:", err)
		return
	}
	if objectId.Hex() == "" {
		t.Error("PutObjectShardMeta id:", objectId.Hex())
	}

	// 获取分片元数据
	if shardMeta, err = DMongo.GetShardMetaByIndex("test_object", 0); err != nil {
		t.Error(err)
	}
	if shardMeta.Hash != "test_shard_hash" {
		t.Error("get meta failed:", shardMeta)
		t.Error(shardMeta.Hash == "")
	}

	// 删除分片元数据
	if deleteCount, err = DMongo.DeleteShardMeta("test_shard_hash"); err != nil {
		t.Error(err)
	}
	if deleteCount != 1 {
		t.Error("delete meta failed, deleteCount:", deleteCount)
	}
	_ = DMongo.Collection.Drop(context.TODO())
}

// -------------------------------
// 测试待修复对象元数据的操作
// -------------------------------
func TestDossMongo_PutRepairShardMeta(t *testing.T) {
	var (
		DMongo     *DossMongo
		insertedID primitive.ObjectID
		err        error
	)

	DMongo = NewDossMongo(funcParams.MongoParamCollection(config.GConfig.RepairObjColName))
	_ = DMongo.Collection.Drop(context.TODO())
	if insertedID, err = DMongo.PutRepairShardMeta(
		"test_object_hash", "2", "test_shard_hash",
	); err != nil {
		t.Error("PutRepairShardMeta failed:", err)
		return
	}
	if insertedID.Hex() == "" {
		t.Error("PutRepairShardMeta id:", insertedID.Hex())
	}

	// 将表drop，恢复环境
	_ = DMongo.Collection.Drop(context.TODO())
}

func TestDossMongo_UpdateRepairShardMeta(t *testing.T) {
	var (
		DMongo    *DossMongo
		shardMeta *RepairShard
		err       error
	)

	DMongo = NewDossMongo(funcParams.MongoParamCollection(config.GConfig.RepairObjColName))
	_ = DMongo.Collection.Drop(context.TODO())

	// 插入一条记录
	_, err = DMongo.PutRepairShardMeta("test_object_hash", "2", "test_shard_hash")
	if err != nil {
		t.Error(err)
	}

	// 修改记录（加locker属性）
	err = DMongo.UpdateRepairShardMeta("test_shard_hash", "192.168.1.51:32000")
	if err != nil {
		t.Error(err)
	}

	// 获取记录
	shardMeta, err = DMongo.GetRepairShardMeta("test_shard_hash")
	if err != nil {
		t.Error(err)
	}
	if shardMeta.Locker != "192.168.1.51:32000" {
		t.Errorf("update error, got %s, expect %s", shardMeta.Locker, "192.168.1.51:32000")
	}

	// 将表drop，恢复环境
	_ = DMongo.Collection.Drop(context.TODO())
}

func TestDossMongo_GetRepairShardMetaByOId(t *testing.T) {
	var (
		DMongo    *DossMongo
		ObjectId  primitive.ObjectID
		shardMeta *RepairShard
		err       error
	)

	DMongo = NewDossMongo(funcParams.MongoParamCollection(config.GConfig.RepairObjColName))
	_ = DMongo.Collection.Drop(context.TODO())

	// 插入一条记录
	ObjectId, err = DMongo.PutRepairShardMeta("test_object_hash", "2", "test_shard_hash")
	if err != nil {
		t.Error(err)
	}

	// 按照objectId获取记录
	if shardMeta, err = DMongo.GetRepairShardMetaByOId(ObjectId); err != nil {
		t.Error(err)
	}
	if shardMeta.ShardHash == "" {
		t.Error("Get shard meta error:", shardMeta)
	}

	// 将表drop，恢复环境
	_ = DMongo.Collection.Drop(context.TODO())
}

func TestDossMongo_GetRepairShardMetaByLocker(t *testing.T) {
	var (
		DMongo *DossMongo
		metas  []*RepairShard
		err    error
	)

	DMongo = NewDossMongo(funcParams.MongoParamCollection(config.GConfig.RepairObjColName))
	_ = DMongo.Collection.Drop(context.TODO())

	// 生成两个待修复分片元数据
	_, _ = DMongo.PutRepairShardMeta("test_object_hash", "1", "test_shard_hash_1")
	_, _ = DMongo.PutRepairShardMeta("test_object_hash", "2", "test_shard_hash_2")

	// 更新locker属性（加锁）
	_ = DMongo.UpdateRepairShardMeta("test_shard_hash_1", "192.168.1.51:32000")
	_ = DMongo.UpdateRepairShardMeta("test_shard_hash_2", "192.168.1.51:32000")

	if metas, err = DMongo.GetRepairShardMetaByLocker("192.168.1.51:32000"); err != nil {
		t.Error(err)
	}
	if len(metas) != 2 {
		t.Error("Got repair meta numbers is", len(metas), "expect: 2")
	}

	// 将表drop，恢复环境
	_ = DMongo.Collection.Drop(context.TODO())
}

func TestDossMongo_DeleteRepairObjectMeta(t *testing.T) {
	var (
		DMongo      *DossMongo
		deleteCount int64
		count       int64
		err         error
	)

	DMongo = NewDossMongo(funcParams.MongoParamCollection(config.GConfig.RepairObjColName))
	_ = DMongo.Collection.Drop(context.TODO())

	// 插入一条记录
	_, err = DMongo.PutRepairShardMeta("test_object_hash", "2", "test_shard_hash")
	if err != nil {
		t.Error(err)
	}

	// 删除记录
	if deleteCount, err = DMongo.DeleteRepairObjectMeta("test_shard_hash"); err != nil {
		t.Error(err)
	}
	if deleteCount != 1 {
		t.Error("delete repair object meta failed, deleteCount:", deleteCount)
	}

	// 获取记录
	if count, err = DMongo.Collection.CountDocuments(context.TODO(), &bsonx.Doc{}); err != nil {
		t.Error(err)
	}
	if count != 0 {
		t.Error("delete repair object meta failed, count:", count)
	}

	// 将表drop，恢复环境
	_ = DMongo.Collection.Drop(context.TODO())
}
