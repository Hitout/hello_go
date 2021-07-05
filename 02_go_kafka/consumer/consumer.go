package main

import (
	"github.com/zngw/log"
	"gxyan.com/go_kafka/kafka"
	"os/signal"
	"runtime"
	"syscall"
)

func main() {
	// 初始化消费者
	err := kafka.InitConsumer("127.0.0.1:9092")
	if err != nil {
		panic(err)
	}
	// 关闭
	defer kafka.ConsumerClose()

	// 监听
	go func() {
		err = kafka.LoopConsumer("Test", TopicCallBack)
		if err != nil {
			panic(err)
		}
	}()

	signal.Ignore(syscall.SIGHUP)
	runtime.Goexit()
}

func TopicCallBack(data []byte) {
	log.Trace("kafka", "Test:"+string(data))
}
