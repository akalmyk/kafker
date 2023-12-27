package kafker

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaConsumer struct {
	BootstrapServers string
	GroupID          string
	Topic            string
	Consumer         *kafka.Consumer
}

func NewKafkaConsumer(bs, gid, topic string) (*KafkaConsumer, error) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": bs,
		"group.id":          gid,
		"auto.offset.reset": "earliest",
	}

	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, err
	}

	c.SubscribeTopics([]string{topic}, nil)

	return &KafkaConsumer{
		BootstrapServers: bs,
		GroupID:          gid,
		Topic:            topic,
		Consumer:         c,
	}, nil
}

func (kc *KafkaConsumer) Consume(msgChan chan []byte) {
	for {
		//fmt.Println("consume kafka")
		ev := kc.Consumer.Poll(100)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			msgChan <- e.Value
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "Error: %v\n", e)
		}
	}
}

func (kc *KafkaConsumer) ConsumeWithContext(ctx context.Context, msgChan chan []byte) error {
	for {
		ev := kc.Consumer.Poll(100)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			msgChan <- e.Value
		case kafka.Error:
			return e
		}

		select {
		case <-ctx.Done():
			return errors.New("context terminated")
		}
	}
}

func (kc *KafkaConsumer) Close() {
	kc.Consumer.Close()
}
