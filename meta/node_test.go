package meta

import (
	"context"
	"fmt"
	"testing"
	"time"

	"config"
	"github.com/mongodb/mongo-go-driver/bson/primitive"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/x/bsonx"
	"meta/funcParams"
)

func TestDossMongo_GetAllNodes(t *testing.T) {
	var (
		DMongo   *DossMongo
		objectId primitive.ObjectID
		nodeOid  = make(map[string]primitive.ObjectID)
		nodes    []*DsNode
		node     *DsNode
		find     bool
		ip       string
		err      error
	)

	DMongo = NewDossMongo(
		funcParams.MongoParamCollection(config.GConfig.NodeColName),
	)
	_ = DMongo.Collection.Drop(context.TODO())

	// 插入两条文档
	if objectId, err = DMongo.AddDsNode("192.168.1.210", 1); err != nil {
		t.Error("Add node failed:", err)
	}
	nodeOid["192.168.1.210"] = objectId
	if objectId, err = DMongo.AddDsNode("192.168.1.211", 2); err != nil {
		t.Error("Add node failed:", err)
	}
	nodeOid["192.168.1.211"] = objectId

	// 获取全部文档并比对是否正确
	if nodes, err = DMongo.GetAllNodes(); err != nil {
		t.Error("Get all nodes failed:", err)
	}
	if len(nodes) != 2 {
		t.Errorf("Get all nodes num failed: Got %d, expect 2", len(nodes))
	}
	for ip, objectId = range nodeOid {
		find = false
		for _, node = range nodes {
			if objectId == node.OId {
				find = true
			}
		}
		if find == false {
			t.Errorf("Not found %s", ip)
		}
	}

	// 将表drop，恢复环境
	_ = DMongo.Collection.Drop(context.TODO())
}

func TestDossMongo_AddNodeRecord(t *testing.T) {
	var (
		DMongo     *DossMongo
		node       *DsNode
		insertedID primitive.ObjectID
		err        error
		countPre   int64
		oidPre     primitive.ObjectID
		countAfter int64
		oidAfter   primitive.ObjectID
	)

	DMongo = NewDossMongo(
		funcParams.MongoParamCollection(config.GConfig.NodeColName),
	)
	_ = DMongo.Collection.Drop(context.TODO())

	// 插入节点对象
	if _, err = DMongo.AddDsNode("192.168.1.210", 1); err != nil {
		t.Error(err)
	}
	if insertedID.Hex() == "" {
		t.Error("Add node object id:", insertedID.Hex())
	}
	countPre, _ = DMongo.Collection.CountDocuments(context.TODO(), &bsonx.Doc{})
	if node, err = DMongo.GetNodeByIp("192.168.1.210"); err != nil {
		t.Error("Get node err:", err)
	}
	oidPre = node.OId

	// 再次插入相同节点对象，检查是否会重复插入
	_, _ = DMongo.AddDsNode("192.168.1.210", 1)
	countAfter, _ = DMongo.Collection.CountDocuments(context.TODO(), &bsonx.Doc{})
	if node, err = DMongo.GetNodeByIp("192.168.1.210"); err != nil {
		t.Error("Get node err:", err)
	}
	oidAfter = node.OId
	if countPre != countAfter {
		t.Error("Repeated insertion")
	}
	if oidPre != oidAfter {
		t.Error("Repeated insertion")
	}

	// 将表drop，恢复环境
	_ = DMongo.Collection.Drop(context.TODO())
}

func TestDossMongo_DeleteDsNode(t *testing.T) {
	var (
		DMongo      *DossMongo
		insertedID  primitive.ObjectID
		deleteCount int64
		err         error
	)

	DMongo = NewDossMongo(
		funcParams.MongoParamCollection(config.GConfig.NodeColName),
	)
	_ = DMongo.Collection.Drop(context.TODO())

	// 插入节点对象
	if _, err = DMongo.AddDsNode("192.168.1.210", 1); err != nil {
		t.Error(err)
	}
	if insertedID.Hex() == "" {
		t.Error("Add node object id:", insertedID.Hex())
	}

	// 删除刚刚插入的节点对象
	if deleteCount, err = DMongo.DeleteDsNodeByIp("192.168.1.210"); err != nil {
		t.Error(err)
	}
	if deleteCount != 1 {
		t.Error("Deleted", deleteCount, "node, expect: 1")
	}

	// 将表drop，恢复环境
	_ = DMongo.Collection.Drop(context.TODO())
}

func TestDossMongo_Watch(t *testing.T) {
	var (
		DMongo       *DossMongo
		changeStream *mongo.ChangeStream
		err          error
		changeCount  = 0
		watchFinish  = make(chan bool)
	)

	DMongo = NewDossMongo(
		funcParams.MongoParamCollection(config.GConfig.NodeColName),
	)
	_ = DMongo.Collection.Drop(context.TODO())

	// 返回*mongo.changeStream
	changeStream, err = DMongo.Collection.Watch(context.TODO(), mongo.Pipeline{})
	if err != nil {
		t.Log(err)
		return
	}
	defer changeStream.Close(context.TODO())

	// 起一个协程插入一条记录
	go func() {
		time.Sleep(time.Second * 3)
		_, _ = DMongo.AddDsNode("192.168.1.210", 1)
	}()

	// 监听到变化流并输出，并增加超时退出机制
	go func() {
		for {
			if changeStream.Next(context.TODO()) {
				changeCount++
				fmt.Println(changeStream.Current)
				watchFinish <- true
			}
		}
	}()

	for {
		select {
		case <-watchFinish:
			_ = DMongo.Collection.Drop(context.TODO())
			return
		case <-time.After(time.Second * 5):
			if changeCount != 1 {
				t.Error("Watch count err: nothing was received in 5 seconds.")
				_ = DMongo.Collection.Drop(context.TODO())
				return
			}
		}
	}
}
