package check

import (
	"github.com/robfig/cron"
)

func SystemDataCheck() {
	var (
		Cron     *cron.Cron
		cronExpr string
	)

	Cron = cron.New()

	// 每天凌晨4点执行数据检查任务，你见过凌晨4点的洛杉矶吗？→_→
	cronExpr = "0 0 4 * * ?"
	_ = Cron.AddFunc(cronExpr, func() {
		MetadataCheck()
		ObjectsCheck()
	})
	Cron.Start()

	select {}
}
