package meta

import (
	"context"

	"github.com/mongodb/mongo-go-driver/bson/primitive"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/x/bsonx"
)

// ------------------------
// 添加Ds节点至集合中
// ------------------------
func (DMongo *DossMongo) AddDsNode(ip string, weight int) (insertedID primitive.ObjectID, err error) {
	var (
		doc    *DsNode
		result *mongo.InsertOneResult
	)

	// 若文档记录已存在，则直接返回
	if doc, _ = DMongo.GetNodeByIp(ip); doc != nil {
		return
	}

	// 构造待上传的BSON文档并进行插入
	doc = &DsNode{
		OId:    primitive.NewObjectID(),
		Ip:     ip,
		Weight: weight,
	}
	if result, err = DMongo.Collection.InsertOne(context.TODO(), doc); err != nil {
		return
	}
	// result.InsertedID类型为interface{}，故需进行类型断言转换为primitive.ObjectID类型
	insertedID = result.InsertedID.(primitive.ObjectID)
	return
}

// ------------------------
// 根据MongoDB objectID查找Ds节点
// ------------------------
func (DMongo *DossMongo) GetAllNodes() (nodes []*DsNode, err error) {
	var (
		cursor *mongo.Cursor
		node   *DsNode
	)

	// 执行Find查询操作
	if cursor, err = DMongo.Collection.Find(context.TODO(), &bsonx.Doc{}); err != nil {
		return
	}
	defer cursor.Close(context.TODO())

	// 解码BSON文档
	for cursor.Next(context.TODO()) {
		node = &DsNode{}
		if err = cursor.Decode(node); err != nil {
			continue
		}
		nodes = append(nodes, node)
	}
	return
}

// ------------------------
// 根据MongoDB objectID查找Ds节点
// ------------------------
func (DMongo *DossMongo) GetNodeByOId(oid primitive.ObjectID) (node *DsNode, err error) {
	var (
		filter *OIdFilter
		result *mongo.SingleResult
	)

	// 过滤条件
	filter = &OIdFilter{
		ObjectId: oid,
	}

	// 执行FindOne查询操作
	if result = DMongo.Collection.FindOne(context.TODO(), filter); result.Err() != nil {
		err = result.Err()
		return
	}
	err = result.Decode(&node)
	return
}

// ------------------------
// 根据ip名查找Ds节点
// ------------------------
func (DMongo *DossMongo) GetNodeByIp(ip string) (node *DsNode, err error) {
	var (
		filter *NodeIpFilter
		result *mongo.SingleResult
	)

	// 过滤条件
	filter = &NodeIpFilter{
		Ip: ip,
	}

	// 执行FindOne查询操作
	if result = DMongo.Collection.FindOne(context.TODO(), filter); result.Err() != nil {
		err = result.Err()
		return
	}
	err = result.Decode(&node)
	return
}

// ------------------------
// 从集合中删除Ds节点
// ------------------------
func (DMongo *DossMongo) DeleteDsNodeByIp(ip string) (deleteCount int64, err error) {
	var (
		filter *NodeIpFilter
		result *mongo.DeleteResult
	)

	// 过滤条件
	filter = &NodeIpFilter{
		Ip: ip,
	}

	// 执行删除操作
	result, err = DMongo.Collection.DeleteOne(context.TODO(), filter)
	if err != nil || result == nil {
		deleteCount = 0
		return
	}
	deleteCount = result.DeletedCount
	return
}
