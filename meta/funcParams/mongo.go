package funcParams

import "config"

// ---------------------------------
// 实现MongoDB操作的可选参数，支持默认值
// EXPLAIN：使用Functional Options Pattern方式
// ---------------------------------

// 定义函数类型：用于修改MongoParams结构体
type MongoParamFunc func(opts *MongoParams)

// MongoDB的选项：DatabaseName: 数据库名; CollectionName: 集合名
type MongoParams struct {
	DatabaseName   string
	CollectionName string
}

// 创建默认参数（DatabaseName、CollectionName）
func newDefaultMongoParams() *MongoParams {
	return &MongoParams{
		DatabaseName:   config.GConfig.DatabaseName,
		CollectionName: config.GConfig.ObjectColName,
	}
}

// 设置DatabaseName属性（通过闭包产生可以修改MongoParams结构体的DatabaseName属性的函数）
// NOTE：构造者模式，将调用处传入的函数进行加工赋予其修改MongoParams的能力，返回给调用处使用
func MongoParamDatabase(DatabaseName string) MongoParamFunc {
	return func(opts *MongoParams) {
		opts.DatabaseName = DatabaseName
	}
}

// 设置CollectionName属性（通过闭包产生可以修改MongoParams结构体的CollectionName属性的函数）
// NOTE：构造者模式，将调用处传入的函数进行加工赋予其修改MongoParams的能力，返回给调用处使用
func MongoParamCollection(CollectionName string) MongoParamFunc {
	return func(params *MongoParams) {
		params.CollectionName = CollectionName
	}
}

// ---------------------------------
// 获取MongoParams结构体（通过传入的函数参数生成MongoParams结构体并返回引用地址）
// ---------------------------------
func NewMongoParams(paramFunctions []MongoParamFunc) (Params *MongoParams) {
	Params = newDefaultMongoParams()
	for _, paramFunc := range paramFunctions {
		paramFunc(Params)
	}
	return
}
