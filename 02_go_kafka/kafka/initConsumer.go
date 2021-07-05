package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"strings"
)

var consumer sarama.Consumer

// 消费者回调函数
type ConsumerCallback func(data []byte)

// 初始化消费者
func InitConsumer(hosts string) (err error) {
	config := sarama.NewConfig()
	client, err := sarama.NewClient(strings.Split(hosts, ","), config)
	if err != nil {
		return fmt.Errorf("unable to create kafka client: ", err)
	}

	consumer, err = sarama.NewConsumerFromClient(client)
	return err
}

// 消费者循环
func LoopConsumer(topic string, callback ConsumerCallback) (err error) {
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		return err
	}
	defer partitionConsumer.Close()

	for {
		msg := <-partitionConsumer.Messages()
		if callback != nil {
			callback(msg.Value)
		}
	}
}

func ConsumerClose() {
	if consumer != nil {
		consumer.Close()
	}
}
