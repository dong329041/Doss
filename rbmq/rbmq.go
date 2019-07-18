package rbmq

import (
	"encoding/json"

	"github.com/streadway/amqp"
	"rbmq/funcParams"
)

// --------------------------------
// Producer方法定义
// --------------------------------
func (pro *Producer) Publish(exchange string, body interface{}) {
	var (
		strBytes []byte
		err      error
	)

	strBytes, err = json.Marshal(body)
	if err != nil {
		panic(err)
	}

	err = pro.Channel.Publish(
		exchange,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        strBytes,
		},
	)
	// if err != nil {
	// 	panic(err)
	// }
}

func (pro *Producer) Close() {
	_ = pro.Channel.Close()
	_ = pro.Conn.Close()
}

// --------------------------------
// Consumer方法定义
// --------------------------------
// DeclareExchange call方式：
//   1) DeclareExchange(exchangeName): 声明交换机 (使用默认类型，为direct)
//   2) DeclareExchange(exchangeName, ExchangeParamType("topic")): 声明交换机 (类型为topic)
func (con *Consumer) DeclareExchange(exchange string, paramFunc ...funcParams.ExchangeParamFunc) {
	var (
		exchangeParams *funcParams.ExchangeParams
		err            error
	)

	// 获取参数（exchangeType）
	exchangeParams = funcParams.NewExchangeParams(paramFunc)

	if err = con.Channel.ExchangeDeclare(
		exchange,            // name
		exchangeParams.Type, // type
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // noWait
		nil,                 // arguments
	); err != nil {
		panic(err)
	}
}

func (con *Consumer) QueueBind(exchange string) {
	var err error

	if err = con.Channel.QueueBind(
		con.QueueName, // queue name
		"",            // routing key
		exchange,      // exchange
		false,         // no-wait
		nil,           // arguments
	); err != nil {
		panic(err)
	}

	con.Exchange = exchange
}

func (con *Consumer) Consume() <-chan amqp.Delivery {
	var (
		delivery <-chan amqp.Delivery
		err      error
	)

	if delivery, err = con.Channel.Consume(
		con.QueueName, // queue
		"",            // consumer
		true,          // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // arguments
	); err != nil {
		panic(err)
	}

	return delivery
}

func (con *Consumer) Close() {
	_ = con.Channel.Close()
	_ = con.Conn.Close()
}
