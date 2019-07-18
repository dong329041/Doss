package utils

import (
	"common"
	"github.com/fsnotify/fsnotify"
	"log"
	"time"
)

// --------------------------------------------
// 监控指定目录文件的变化
// Param: watchPath: 被监听的目录路径; callBackFunc: 当目录发生改变时的回调函数
// NOTE: 监听的操作类型：Write、Remove
// NOTICE: 增加函数防抖处理，当目录下文件发生改变时，将该事件收集起来，启动定时器5秒
//         若5秒内发生其他变化，则继续将该事件收集起来，并将定时器重置
//         直至5秒内没有再发生其他变化，遍历收集到的所有事件，找到所有被改变的文件
// --------------------------------------------
var stopWatchSignal = make(chan bool)

// 获取停止监控信号的通道（向该通道中放入true，即可结束监听）
func GetStopWatchSignal() *chan bool {
	return &stopWatchSignal
}

// 监控指定目录文件的变化（阻塞，循环监听）
func WatchObjects(watchPath string, callbackFunc func([]string)) {
	var (
		watcher *fsnotify.Watcher
		files   []string
		event   fsnotify.Event
		timer   *time.Timer
		ok      bool
		err     error
	)

	// 生成文件系统监控watcher
	if watcher, err = fsnotify.NewWatcher(); err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	// 将被监控的目录添加到watcher
	if err = watcher.Add(watchPath); err != nil {
		log.Fatal(common.ErrWatchFilePath, err)
	}

	// 生成一个未启动的定时器
	timer = time.NewTimer(5 * time.Second)
	timer.Stop()

	// 在协程中监控目录改变
	go func() {
		for {
			select {
			case event, ok = <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Remove == fsnotify.Remove {
					files = append(files, event.Name)
					timer.Reset(5 * time.Second)
				}
			case err, ok = <-watcher.Errors:
				if !ok {
					return
				}
				log.Println(common.ErrWatcherEvent, err)
			}
		}
	}()

	// 在协程中接收定时器到时的消息，并将发生变化的files切片去重后传入回调函数执行
	for {
		select {
		case <-timer.C:
			callbackFunc(SliceRemoveReplica(files))
			files = []string{}
		case <-stopWatchSignal:
			return
		}
	}
}
