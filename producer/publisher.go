package producer

import (
	"github.com/Sirupsen/logrus"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

type Publisher struct {
	brokers   string
	topic     string
	producer  *kafka.Producer
	closeChan chan bool
	closed    bool
	logger    *logrus.Logger
}

func NewPublisher(brokers, topic string, logger *logrus.Logger) (*Publisher, error) {
	p := &Publisher{
		brokers:   brokers,
		topic:     topic,
		logger:    logger,
		closed:    false,
		closeChan: make(chan bool),
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":      p.brokers,
		"go.delivery.reports":    false,
		"go.batch.producer":      true,
		"queue.buffering.max.ms": 1,
	})

	if err != nil {
		p.logger.Errorf("Failed to create producer: %s", err)
		return nil, err
	}

	p.logger.Infof("Created Producer %v", producer)
	p.producer = producer
	return p, nil
}

func (p *Publisher) Close() {
	p.closed = true
	p.producer.Close()
	close(p.closeChan)
}

func (p *Publisher) Flush(timeoutMs int) int {
	return p.producer.Flush(timeoutMs)
}

func (p *Publisher) PublishBatch(values []string) {
	// TODO Not using under layer API
	for _, value := range values {
		p.Publish(value)
	}
}

func (p *Publisher) PublishSync(value string) {
	deliveryChan := make(chan kafka.Event)

	if err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &p.topic,
			Partition: kafka.PartitionAny,
		}, Value: []byte(value)}, deliveryChan); err != nil {
		p.logger.Errorf("enque message error %s", err)
		return
	}
	start := time.Now()
	e := <-deliveryChan
	elapsed := time.Since(start)
	p.logger.Infof("Pooling from channel took %s", elapsed)

	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		p.logger.Errorf("Delivery failed: %v", m.TopicPartition.Error)
	} else {
		//p.logger.Debugf("Delivered message to topic %s#%d#%d",
		//	*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
	close(deliveryChan)
}

func (p *Publisher) Publish(value string) {
	var doneChan chan bool
	syncMode := false

	if syncMode {
		doneChan = make(chan bool)

		go func() {
		outer:
			for e := range p.producer.Events() {
				switch ev := e.(type) {
				case *kafka.Message:
					m := ev
					if m.TopicPartition.Error != nil {
						p.logger.Warnf("Delivery failed: %v", m.TopicPartition.Error)
					} else {
						p.logger.Debugf("Delivered message to topic %s#%d#%d",
							*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					}
					break outer

				default:
					p.logger.Warnf("Unknown event: %s", ev)
				}
			}

			close(doneChan)
		}()
	}

	//go func() {
	//
	//	run := true
	//	for run {
	//		select {
	//		case e := <-p.producer.Events():
	//			switch ev := e.(type) {
	//			case *kafka.Message:
	//				m := ev
	//				if m.TopicPartition.Error != nil {
	//					p.logger.Warnf("Delivery failed: %v", m.TopicPartition.Error)
	//				} else {
	//					p.logger.Debugf("Delivered message to topic %s#%d#%d",
	//						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	//				}
	//
	//			//default:
	//			//	p.logger.Warnf("Unknown event: %s", ev)
	//			}
	//
	//		case  <-p.closeChan:
	//			//p.logger.Infof("Caught signal %v: terminating", sig)
	//		// stop producer
	//			run = false
	//		}
	//	}
	//}()

	if !p.closed {
		p.producer.ProduceChannel() <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &p.topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(value),
		}
	}

	if syncMode {
		// wait for delivery report goroutine to finish
		_ = <-doneChan
	}
}
