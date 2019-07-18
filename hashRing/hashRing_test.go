package hashRing

import (
	"fmt"
	"sort"
	"strconv"
	"testing"

	"config"
	"github.com/mongodb/mongo-go-driver/bson/primitive"
)

func checkEqual(num, expected interface{}, t *testing.T) {
	if num != expected {
		t.Errorf("value is %d, expected %d", num, expected)
	}
}

func TestNewHashRing(t *testing.T) {
	var ring *HashRing
	if ring = InitHashRing(); ring == nil {
		t.Error("InitHashRing failed.")
	}
}

func TestHashRing_AddNode(t *testing.T) {
	var ring *HashRing
	ring = InitHashRing()
	AddNode(primitive.NewObjectID(), "192.168.1.10", 1)
	checkEqual(len(ringMap), config.GConfig.DefaultVirtualCubes, t)
	checkEqual(len(sortedRing), config.GConfig.DefaultVirtualCubes, t)
	if sort.IsSorted(sortedRing) == false {
		t.Error("expected sorted ring to be sorted")
	}
	_ = InitHashRing()
}

func TestSetCubeNumber(t *testing.T) {
	var (
		ring *HashRing
		err  error
	)
	ring = InitHashRing()
	if err = SetCubeNumber(40); err != nil {
		t.Error("TestSetCubeNumber err: ", err)
	}
	checkEqual(numberOfCubes, 40, t)

	AddNode(primitive.NewObjectID(), "192.168.1.10", 1)
	checkEqual(len(ringMap), 40, t)

	_ = InitHashRing()
}

func TestHashRing_AddNodes(t *testing.T) {
	var (
		ring      *HashRing
		Nodes     map[string]int
		ObjectIds map[string]primitive.ObjectID
		i         int
		ip        string
	)
	ring = InitHashRing()
	Nodes = make(map[string]int)
	ObjectIds = make(map[string]primitive.ObjectID)

	for i = 0; i < 10; i++ {
		ip = "192.168.1." + strconv.Itoa(i+1)
		Nodes[ip] = i + 1
		ObjectIds[ip] = primitive.NewObjectID()
	}
	AddNodes(Nodes, ObjectIds)
	checkEqual(len(ringMap), config.GConfig.DefaultVirtualCubes*55, t)
	checkEqual(len(sortedRing), config.GConfig.DefaultVirtualCubes*55, t)
	if sort.IsSorted(sortedRing) == false {
		t.Errorf("expected sorted ring to be sorted")
	}

	_ = InitHashRing()
}

func TestHashRing_RemoveNode(t *testing.T) {
	var (
		ring      *HashRing
		Nodes     map[string]int
		ObjectIds map[string]primitive.ObjectID
		i         int
		ip        string
	)
	ring = InitHashRing()
	AddNode(primitive.NewObjectID(), "192.168.1.10", 1)
	RemoveNode("192.168.1.10")
	checkEqual(len(ringMap), 0, t)
	checkEqual(len(sortedRing), 0, t)

	Nodes = make(map[string]int)
	ObjectIds = make(map[string]primitive.ObjectID)
	for i = 0; i < 10; i++ {
		ip = "192.168.1." + strconv.Itoa(i+1)
		Nodes[ip] = i + 1
		ObjectIds[ip] = primitive.NewObjectID()
	}
	AddNodes(Nodes, ObjectIds)
	checkEqual(len(ringMap), 7040, t)
	RemoveNode("192.168.1.10")
	checkEqual(len(ringMap), 5760, t)

	_ = InitHashRing()
}

func TestHashRing_Members(t *testing.T) {
	var (
		Nodes     map[string]int
		ObjectIds map[string]primitive.ObjectID
		i         int
		ip        string
	)
	_ = InitHashRing()

	Nodes = make(map[string]int)
	ObjectIds = make(map[string]primitive.ObjectID)
	for i = 0; i < 10; i++ {
		ip = "192.168.1." + strconv.Itoa(i+1)
		Nodes[ip] = i + 1
		ObjectIds[ip] = primitive.NewObjectID()
	}
	AddNodes(Nodes, ObjectIds)
	checkEqual(len(Members()), 10, t)

	_ = InitHashRing()
}

func TestHashRing_GetNode(t *testing.T) {
	testGet := []struct {
		in, out string
	}{
		{"key1", "192.168.1.3"},
		{"key2", "192.168.1.7"},
		{"key3", "192.168.1.7"},
		{"key4", "192.168.1.9"},
		{"key5", "192.168.1.10"},
	}
	testGetAfterRemove := []struct {
		in, out string
	}{
		{"key1", "192.168.1.3"},
		{"key2", "192.168.1.7"},
		{"key3", "192.168.1.7"},
		{"key4", "192.168.1.9"},
		{"key5", "192.168.1.3"},
	}

	var (
		Nodes     map[string]int
		ObjectIds map[string]primitive.ObjectID
		i         int
		ip        string
		node      string
		err       error
	)
	_ = InitHashRing()

	Nodes = make(map[string]int)
	ObjectIds = make(map[string]primitive.ObjectID)
	for i = 0; i < 10; i++ {
		ip = "192.168.1." + strconv.Itoa(i+1)
		Nodes[ip] = i + 1
		ObjectIds[ip] = primitive.NewObjectID()
	}
	AddNodes(Nodes, ObjectIds)

	for i, v := range testGet {
		node, err = GetNode(v.in)
		if err != nil {
			t.Fatal(i, "err: ", err)
		}
		if node != v.out {
			t.Error("index", i, "err: got", node, ", expected", v.out)
		}
	}

	RemoveNode("192.168.1.10")
	for i, v := range testGetAfterRemove {
		node, err = GetNode(v.in)
		if err != nil {
			t.Fatal(i, "err: ", err)
		}
		if node != v.out {
			t.Error("index", i, "err: got", node, ", expected", v.out)
		}
	}

	_ = InitHashRing()
}

func TestHashRing_GetNodes(t *testing.T) {
	var (
		Nodes     map[string]int
		ObjectIds map[string]primitive.ObjectID
		i         int
		ip        string
		nodes     []string
		err       error
	)

	_ = InitHashRing()
	Nodes = make(map[string]int)
	ObjectIds = make(map[string]primitive.ObjectID)
	for i = 0; i < 10; i++ {
		ip = "192.168.1." + strconv.Itoa(i+1)
		Nodes[ip] = i + 1
		ObjectIds[ip] = primitive.NewObjectID()
	}
	AddNodes(Nodes, ObjectIds)

	nodes, err = GetNodes("hello", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 3 {
		t.Error("expected 3 members instead of", len(nodes))
	}
	if nodes[0] != "192.168.1.3" {
		t.Error("First node error, expected 192.168.1.3, but got", nodes[0])
	}
	if nodes[1] != "192.168.1.5" {
		t.Error("Second node error, expected 192.168.1.5, but got", nodes[1])
	}
	if nodes[2] != "192.168.1.4" {
		t.Error("Third node error, expected 192.168.1.4, but got", nodes[2])
	}

	AddNode(primitive.NewObjectID(), "192.168.1.20", 10)
	nodes, err = GetNodes("hello", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 3 {
		t.Error("expected 3 members instead of", len(nodes))
	}
	if nodes[0] != "192.168.1.3" {
		t.Error("First node error, expected 192.168.1.3, but got", nodes[0])
	}
	if nodes[1] != "192.168.1.20" {
		t.Error("Second node error, expected 192.168.1.20, but got", nodes[1])
	}
	if nodes[2] != "192.168.1.5" {
		t.Error("Third node error, expected 192.168.1.5, but got", nodes[2])
	}

	// r.RemoveNode("192.168.1.3")
	// nodes, err = r.GetNodes("key1", 3)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// if len(nodes) != 3 {
	// 	t.Error("expected 3 members instead of", len(nodes))
	// }
	// if nodes[0] != "192.168.1.5" {
	// 	t.Error("First node error, expected 192.168.1.3, but got", nodes[0])
	// }
	// if nodes[1] != "192.168.1.7" {
	// 	t.Error("Second node error, expected 192.168.1.5, but got", nodes[1])
	// }
	// if nodes[2] != "192.168.1.8" {
	// 	t.Error("Third node error, expected 192.168.1.7, but got", nodes[2])
	// }

	_ = InitHashRing()
}

func TestHashRing_Balance(t *testing.T) {
	var (
		Nodes           map[string]int
		ObjectIds       map[string]primitive.ObjectID
		i               int
		ip              string
		nodeMap         map[string]int
		key             string
		node            string
		ok              bool
		expectedNodeMap map[string]int
		k               string
		v               int
	)

	_ = InitHashRing()
	Nodes = make(map[string]int)
	ObjectIds = make(map[string]primitive.ObjectID)
	for i = 0; i < 10; i++ {
		ip = "192.168.1." + strconv.Itoa(i+1)
		Nodes[ip] = i + 1
		ObjectIds[ip] = primitive.NewObjectID()
	}
	AddNodes(Nodes, ObjectIds)

	nodeMap = make(map[string]int, 0)
	for i = 0; i < 10000; i++ {
		key = fmt.Sprintf("key%d", i)
		node, _ = GetNode(key)
		if _, ok = nodeMap[node]; ok {
			nodeMap[node] += 1
		} else {
			nodeMap[node] = 1
		}
	}

	expectedNodeMap = make(map[string]int)
	expectedNodeMap["192.168.1.1"] = 130
	expectedNodeMap["192.168.1.2"] = 366
	expectedNodeMap["192.168.1.3"] = 463
	expectedNodeMap["192.168.1.4"] = 623
	expectedNodeMap["192.168.1.5"] = 987
	expectedNodeMap["192.168.1.6"] = 1009
	expectedNodeMap["192.168.1.7"] = 1465
	expectedNodeMap["192.168.1.8"] = 1333
	expectedNodeMap["192.168.1.9"] = 1578
	expectedNodeMap["192.168.1.10"] = 2046

	for k, v = range nodeMap {
		if v != expectedNodeMap[k] {
			t.Error(k, "key quantity error: got", v, ", expected", expectedNodeMap[k])
		}
	}

	_ = InitHashRing()
}
