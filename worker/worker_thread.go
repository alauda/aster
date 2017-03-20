package worker

import (
	"github.com/Sirupsen/logrus"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"sync"
	"time"
)

type workerThread struct {
	consumerWorker    *ConsumerWorker
	id                int64
	logger            *logrus.Entry // From Worker
	topic             string        // From Worker
	stopChan          chan bool
	consumer          *kafka.Consumer
	waitGroup         *sync.WaitGroup
	statsMutex        *sync.Mutex
	errors            int64
	succeed           int64
	reportStatsTicker *time.Ticker
}

func newWorkerThread(w *ConsumerWorker, index int64) *workerThread {
	return &workerThread{
		consumerWorker:    w,
		id:                (index + 1),
		stopChan:          make(chan bool),
		waitGroup:         w.waitGroup,
		topic:             w.topic,
		statsMutex:        &sync.Mutex{},
		errors:            0,
		succeed:           0,
		reportStatsTicker: time.NewTicker(30 * time.Second),
	}
}

func (wt *workerThread) stop() {
	wt.reportStats()
	close(wt.stopChan)
}

func (wt *workerThread) start() {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               wt.consumerWorker.broker,
		"group.id":                        wt.consumerWorker.group,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"}})

	if err != nil {
		wt.logger.Errorf("Failed to create consumer: %s", err.Error())
		wt.waitGroup.Done()
		return
	}
	wt.consumer = c
	wt.logger.Infof("Created Consumer %v", c)

	if err = wt.consumer.Subscribe(wt.topic, nil); err != nil {
		wt.logger.Errorf("Error when subscribe topic %s, error: %s",
			wt.topic, err.Error())
		wt.waitGroup.Done()
		return
	}
	wt.logger.Infof("Subscribed to topic %s", wt.topic)

	running := true
	for running == true {
		select {
		case ev := <-wt.consumer.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				wt.logger.Debugf("[Event:AssignedPartitions] %v", e)
				wt.consumer.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				wt.logger.Debugf("[Event:RevokedPartitions] %v", e)
				wt.consumer.Unassign()
			case *kafka.Message:
				//wt.logger.Debugf("[Event:Message] %s#%d#%d: %s",
				//	*e.TopicPartition.Topic, e.TopicPartition.Partition,
				//	e.TopicPartition.Offset, string(e.Value))
				if e := wt.consumerWorker.messageProcessor.ProcessMessage(e); e != nil {
					wt.errors++
				} else {
					wt.succeed++
				}
				if wt.succeed%1000 == 0 {
					wt.logger.Infof("consumed %d messages", wt.succeed)
				}
			case kafka.PartitionEOF:
			//fmt.Printf("%% [worker-%d] Reached %v\n", wt.id,e)
			case kafka.Error:
				wt.logger.Errorf("[Event:Error] %v", e)
			}
		case <-wt.stopChan:
			running = false
		case <-wt.reportStatsTicker.C:
			wt.reportStats()
		}

	}

	wt.logger.Info("Closing consumer")
	if err := wt.consumer.Close(); err != nil {
		wt.logger.Errorf("close consumer error %s", err)
	}
	wt.logger.Infof("message consumed %d", wt.succeed)
	wt.waitGroup.Done()
	wt.logger.Info("Worker closed")
}

func (wt *workerThread) reportStats() {
	wt.statsMutex.Lock()
	defer wt.statsMutex.Unlock()
	wt.logger.Infof("processed %d, error %d, succeed %d", (wt.errors + wt.succeed), wt.errors, wt.succeed)
	wt.errors = 0
	wt.succeed = 0
}

func (wt *workerThread) IncErrors(errors int) {
	wt.statsMutex.Lock()
	defer wt.statsMutex.Unlock()
	wt.errors = wt.errors + int64(errors)
	wt.consumerWorker.IncErrors(errors)
}

func (wt *workerThread) IncSucceed(succeed int) {
	wt.statsMutex.Lock()
	defer wt.statsMutex.Unlock()
	wt.succeed = wt.succeed + int64(succeed)
	wt.consumerWorker.IncSucceed(succeed)
}
