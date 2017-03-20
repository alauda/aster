package worker

import (
	"github.com/Sirupsen/logrus"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"sync"
	"time"
)

// MessageProcessor will ne called when new message arrived
type MessageProcessor interface {
	ProcessMessage(*kafka.Message) error
	StartDaemon() error
	StopDaemon() error
}

type ConsumerWorker struct {
	broker            string
	group             string
	topic             string
	threads           int64
	logger            *logrus.Entry
	stopChan          chan bool
	waitGroup         *sync.WaitGroup
	statsMutex        *sync.Mutex
	errors            int64
	succeed           int64
	messageProcessor  MessageProcessor // Called after new message arrived
	workerThreads     []*workerThread
	reportStatsTicker *time.Ticker
}

func (w *ConsumerWorker) Stop() error {

	w.logger.Infof("begin stop consumer worker daemonThread")
	if err := w.messageProcessor.StopDaemon(); err != nil {
		w.logger.Errorf("failt to StopDaemon %s", err)
	}

	w.logger.Infof("begin stop consumer worker thread")
	for _, wt := range w.workerThreads {
		wt.stop()
	}
	w.logger.Infof("end stop consumer worker thread")

	close(w.stopChan)

	return nil
}

func (w *ConsumerWorker) Wait() {
	w.waitGroup.Wait()
}

func (w *ConsumerWorker) Start() error {
	if err := w.messageProcessor.StartDaemon(); err != nil {
		return err
	}

	var i int64
	for i = 0; i < w.threads; i++ {
		wt := newWorkerThread(w, i)
		logger := w.logger.WithField("worker-id", wt.id)
		wt.logger = logger
		w.workerThreads = append(w.workerThreads, wt)
		w.waitGroup.Add(1)
		go wt.start()
	}

	go func() {
	outer:
		for {
			select {
			case <-w.reportStatsTicker.C:
				w.printStats()
			case <-w.stopChan:
				break outer
			}
		}
		w.logger.Infof("ConsumerWorker report stats goroutine exit")
	}()

	return nil
}

func NewConsumerWorker(broker, group, topic string,
	threads int64, messageProcessor MessageProcessor,
	logger *logrus.Entry) (*ConsumerWorker, error) {

	w := ConsumerWorker{
		broker:            broker,
		group:             group,
		topic:             topic,
		threads:           threads,
		messageProcessor:  messageProcessor,
		waitGroup:         &sync.WaitGroup{},
		logger:            logger,
		workerThreads:     []*workerThread{},
		errors:            0,
		succeed:           0,
		statsMutex:        &sync.Mutex{},
		reportStatsTicker: time.NewTicker(60 * time.Second),
	}
	w.stopChan = make(chan bool)
	logger.Debugf("NewConsumerWorker(broker: %s, group: %s, topic: %s, threads: %d)",
		broker, group, topic, threads)
	return &w, nil
}

func (w *ConsumerWorker) printStats() {
	w.statsMutex.Lock()
	defer w.statsMutex.Unlock()
	w.logger.Infof("processed %d, error %d, succeed %d", (w.errors + w.succeed), w.errors, w.succeed)
	w.errors = 0
	w.succeed = 0
}

func (w *ConsumerWorker) IncErrors(errors int) {
	w.statsMutex.Lock()
	defer w.statsMutex.Unlock()
	w.errors = w.errors + int64(errors)
}

func (w *ConsumerWorker) IncSucceed(succeed int) {
	w.statsMutex.Lock()
	defer w.statsMutex.Unlock()
	w.succeed = w.succeed + int64(succeed)

}
