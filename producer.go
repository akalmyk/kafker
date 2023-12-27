package kafker

import (
	"errors"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaProducer struct {
	BootstrapServers string
	Topic            string
	Producer         *kafka.Producer
}

func NewKafkaProducer(bs, gid, topic string) (*KafkaProducer, error) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": bs,
	}

	p, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{
		BootstrapServers: bs,
		Topic:            topic,
		Producer:         p,
	}, nil
}

func (kp *KafkaProducer) Close() {
	kp.Producer.Close()
}

func (kp *KafkaProducer) Produce(message string) error {
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	err := kp.Producer.Produce(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &kp.Topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(message),
		}, deliveryChan,
	)

	if err != nil {
		return err
	}

	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				return ev.TopicPartition.Error
			} else {
				return nil
			}
		}
	}

	return errors.New("nothing happened")
}
