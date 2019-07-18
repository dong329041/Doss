package rbmq

import (
	"encoding/json"
	"testing"

	"github.com/streadway/amqp"
	"rbmq/funcParams"
	"utils"
)

var RabbitMQUrl = utils.GetRabbitMqUrl()

func checkEqual(num, expected interface{}, t *testing.T) {
	if num != expected {
		t.Errorf("value is %d, expected %d", num, expected)
	}
}

func TestPublishAndConsume(t *testing.T) {
	var (
		channel  <-chan amqp.Delivery
		channel2 <-chan amqp.Delivery
		msg      amqp.Delivery
		err      error
		con1     *Consumer
		con2     *Consumer
		pro1     *Producer
	)

	const (
		Message      = "message_test"
		ExchangeName = "exchange_test"
		ExchangeType = "topic"
	)

	// 创建生产者消费者
	pro1 = NewProducer(RabbitMQUrl)
	defer pro1.Close()

	con1 = NewConsumer(RabbitMQUrl)
	defer con1.Close()
	con1.DeclareExchange(ExchangeName, funcParams.ExchangeParamType(ExchangeType))
	con1.QueueBind(ExchangeName)

	con2 = NewConsumer(RabbitMQUrl)
	defer con2.Close()
	con2.DeclareExchange(ExchangeName, funcParams.ExchangeParamType(ExchangeType))
	con2.QueueBind(ExchangeName)

	// 生产者发布消息，程序执行结束时将测试用exchange删除
	pro1.Publish(ExchangeName, Message)
	defer pro1.Channel.ExchangeDelete(ExchangeName, false, false)

	// 消费者1：订阅消费消息
	channel = con1.Consume()
	msg = <-channel
	var actual interface{}
	err = json.Unmarshal(msg.Body, &actual)
	if err != nil {
		t.Error(err)
	}
	checkEqual(actual, Message, t)

	// 消费者2：订阅消费消息
	channel2 = con2.Consume()
	msg = <-channel2
	err = json.Unmarshal(msg.Body, &actual)
	if err != nil {
		t.Error(err)
	}
	checkEqual(actual, Message, t)
}
