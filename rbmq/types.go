package rbmq

import "github.com/streadway/amqp"

type Producer struct {
	Conn     *amqp.Connection
	Channel  *amqp.Channel
	Exchange string
}

type Consumer struct {
	Conn      *amqp.Connection
	Channel   *amqp.Channel
	QueueName string
	Exchange  string
}

func NewProducer(url string) *Producer {
	var (
		err  error
		conn *amqp.Connection
		ch   *amqp.Channel
	)

	if conn, err = amqp.Dial(url); err != nil {
		panic(err)
	}
	if ch, err = conn.Channel(); err != nil {
		panic(err)
	}

	return &Producer{
		Conn:    conn,
		Channel: ch,
	}
}

func NewConsumer(url string) *Consumer {
	var (
		err   error
		conn  *amqp.Connection
		ch    *amqp.Channel
		queue amqp.Queue
	)

	if conn, err = amqp.Dial(url); err != nil {
		panic(err)
	}
	if ch, err = conn.Channel(); err != nil {
		panic(err)
	}
	if queue, err = ch.QueueDeclare(
		"",    // name
		false, // durable
		true,  // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	); err != nil {
		panic(err)
	}

	return &Consumer{
		Conn:      conn,
		Channel:   ch,
		QueueName: queue.Name,
	}
}
