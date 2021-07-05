package main

import (
	"gxyan.com/go_kafka/kafka"
	"os/signal"
	"runtime"
	"syscall"
)

func main() {
	// 初始化生产生
	err := kafka.InitProducer("127.0.0.1:9092")
	if err != nil {
		panic(err)
	}

	// 关闭
	defer kafka.ProductClose()

	// 发送测试消息
	kafka.Send("Test", "This is Test Msg")
	kafka.Send("Test", "Hello Guoke")

	signal.Ignore(syscall.SIGHUP)
	runtime.Goexit()
}
