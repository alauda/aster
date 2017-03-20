package producer

import (
	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
	"time"
)

type SaramaPublisher struct {
	brokers  string
	topic    *string
	succeed  int64
	producer sarama.SyncProducer
	logger   *logrus.Logger
}

func NewSaramaPublisher(brokers, topic string, logger *logrus.Logger) (*SaramaPublisher, error) {
	saramaPublisher := &SaramaPublisher{
		brokers: brokers,
		topic:   &topic,
		succeed: 0,
		logger:  logger,
	}

	producer, err := sarama.NewSyncProducer([]string{saramaPublisher.brokers}, nil)
	if err != nil {
		return nil, err
	}
	saramaPublisher.producer = producer

	return saramaPublisher, nil

}

func (sp *SaramaPublisher) Close() error {
	sp.logger.Infof("succeed %d", sp.succeed)
	return sp.producer.Close()
}

func (sp *SaramaPublisher) Publish(value string) error {
	msg := &sarama.ProducerMessage{Topic: *sp.topic, Value: sarama.StringEncoder(value)}
	start := time.Now()

	_, _, err := sp.producer.SendMessage(msg)
	elapsed := time.Since(start)
	sp.logger.Infof("Pooling from channel took %s", elapsed)
	if err != nil {
		sp.logger.Errorf("FAILED to send message: %s", err)
	} else {
		sp.succeed++
		//sp.logger.Debugf("message sent to partition %d at offset %d", partition, offset)
	}
	return err
}
