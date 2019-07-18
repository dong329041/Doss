package funcParams

// ---------------------------------
// 实现元数据操作的可选参数，支持默认值
// EXPLAIN：使用Functional Options Pattern方式
// ---------------------------------

// 定义函数类型：用于修改MetaParams结构体
type MetaParamFunc func(opts *MetaParams)

// MongoDB的选项：Version: 版本号
type MetaParams struct {
	Version int
}

// 创建默认参数（Version默认值为-1，代表最新版本）
func newDefaultMetaParams() *MetaParams {
	return &MetaParams{
		Version: -1,
	}
}

// 设置Version属性（通过闭包产生可以修改MetaParams结构体的Version属性的函数）
// NOTE：构造者模式，将调用处传入的函数进行加工赋予其修改MetaParams的能力，返回给调用处使用
func MetaParamVersion(version int) MetaParamFunc {
	return func(params *MetaParams) {
		params.Version = version
	}
}

// ----------------------------
// 获取MetaParams结构体（通过传入的函数参数生成MetaParams结构体并返回引用地址）
// ----------------------------
func NewMetaParams(paramFunctions []MetaParamFunc) (Params *MetaParams) {
	Params = newDefaultMetaParams()
	for _, paramFunc := range paramFunctions {
		paramFunc(Params)
	}
	return
}
