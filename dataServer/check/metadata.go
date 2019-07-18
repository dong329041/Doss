package check

import (
	"meta"
	"meta/funcParams"
)

// 保留最新的多少个版本
const RemainVersionCount = 5

// 检查元数据：将早期的版本删除，类似队列结构，先入先出
func MetadataCheck() {
	var (
		DMongo      *meta.DossMongo
		repairMetas []*meta.ObjectMeta
		repairMeta  *meta.ObjectMeta
		objMetas    []*meta.ObjectMeta
		i           int
		err         error
	)
	DMongo = meta.NewDossMongo()
	repairMetas, err = DMongo.GetALLTooMuchVersionMeta(RemainVersionCount)
	if err != nil {
		return
	}
	for _, repairMeta = range repairMetas {
		if objMetas, err = DMongo.GetAllVersionMetas(repairMeta.Name); err != nil {
			continue
		}
		for i = 1; i <= len(objMetas)-RemainVersionCount; i++ {
			_, _ = DMongo.DeleteObjectMeta(objMetas[i].Name, funcParams.MetaParamVersion(i))
		}
	}
}
