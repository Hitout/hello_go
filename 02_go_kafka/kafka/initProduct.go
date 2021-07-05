package kafka

import (
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/zngw/log"
)

var producer sarama.AsyncProducer

// 初始化生产者
func InitProducer(hosts string) (err error) {
	config := sarama.NewConfig()
	client, err := sarama.NewClient(strings.Split(hosts, ","), config)
	if err != nil {
		return fmt.Errorf("unable to create kafka client: ", err)
	}
	producer, err = sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return err
	}
	return
}

// 发送消息
func Send(topic, data string) {
	producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(data)}
	log.Trace("kafka", "Produced message: ["+data+"]")
}

func ProductClose() {
	if producer != nil {
		producer.Close()
	}
}
