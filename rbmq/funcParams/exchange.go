package funcParams

import "config"

// ---------------------------------
// 实现元数据操作的可选参数，支持默认值
// EXPLAIN：使用Functional Options Pattern方式
// ---------------------------------

// 定义函数类型：用于修改ExchangeParams结构体
type ExchangeParamFunc func(opts *ExchangeParams)

// MongoDB的选项：Version: 版本号
type ExchangeParams struct {
	Type string
}

// 创建默认参数（Version默认值为-1，代表最新版本）
func newDefaultExchangeParam() *ExchangeParams {
	return &ExchangeParams{
		Type: config.GConfig.ExchangeType,
	}
}

// 设置Version属性（通过闭包产生可以修改ExchangeParams结构体的Version属性的函数）
// NOTE：构造者模式，将调用处传入的函数进行加工赋予其修改ExchangeParams的能力，返回给调用处使用
func ExchangeParamType(exchangeType string) ExchangeParamFunc {
	return func(params *ExchangeParams) {
		params.Type = exchangeType
	}
}

// ----------------------------
// 获取ExchangeParams结构体（通过传入的函数参数生成ExchangeParams结构体并返回引用地址）
// ----------------------------
func NewExchangeParams(paramFunctions []ExchangeParamFunc) (Params *ExchangeParams) {
	Params = newDefaultExchangeParam()
	for _, paramFunc := range paramFunctions {
		paramFunc(Params)
	}
	return
}
