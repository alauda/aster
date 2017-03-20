package main

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/alauda/aster/common"
	"github.com/alauda/aster/daemon"
	"github.com/alauda/aster/logs"
	"github.com/alauda/aster/utils"
	"github.com/alauda/aster/worker"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	WORKER_NAME = "my-worker"
)

func main() {

	os.Setenv(common.ENV_KEY_BROKERS, "localhost:9092")
	os.Setenv(common.ENV_KEY_WORKERS, WORKER_NAME)

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger, _ := logs.SetupLogger(logs.LogConfig{Stdout: true})

	daemon := daemon.NewDaemon(logger)
	run := true

	if err := daemon.Start(); err != nil {
		logger.Fatalf("start daemon error: %s", err)
	}

	for run {
		select {
		case sig := <-sigChan:
			logger.Infof("Caught signal %v: terminating", sig)
			run = false
		}
	}

	// stop daemon
	daemon.Stop()
	daemon.Wait()
	logger.Info("app will exist!")

	logger.Info("app will exist!")
}

type myProcessor struct {
	logger    *logrus.Entry
	mutex     *sync.Mutex
	closeChan chan bool
}

func (p *myProcessor) ProcessMessage(message *kafka.Message) error {
	p.logger.Debugf("Receive: %s", string(message.Value))
	return nil
}

func init() {
	worker.Register(WORKER_NAME, Init)
}

func Init() (*worker.ConsumerWorker, error) {

	brokers := utils.EnvString(common.ENV_KEY_BROKERS, "")
	if len(brokers) == 0 {
		return nil, fmt.Errorf("No brokers provided")
	}

	group := utils.EnvString(fmt.Sprintf(common.ENV_KEY_CONSUMER_GROUP, strings.ToUpper(WORKER_NAME)), "test")
	topic := utils.EnvString(fmt.Sprintf(common.ENV_KEY_TOPIC, strings.ToUpper(WORKER_NAME)), "test")
	threads := utils.EnvInt64(fmt.Sprintf(common.ENV_KEY_THREADS, strings.ToUpper(WORKER_NAME)), 4)

	logConf := logs.LogConfig{
		LogDir:    utils.EnvString(common.ENV_KEY_LOG_DIR, "/var/log/myworker"),
		AppName:   "test-consumer",
		Formatter: logs.DefaultTextFormatter(),
		Level:     logrus.DebugLevel,
	}

	var logger *logrus.Logger
	var err error
	logger, err = logs.SetupLogger(logConf)
	if err != nil {
		return nil, err
	}
	loggerWithField := logger.WithField("worker", "test-consumer")
	processor := &myProcessor{
		logger:    loggerWithField,
		mutex:     &sync.Mutex{},
		closeChan: make(chan bool),
	}
	worker, err := worker.NewConsumerWorker(brokers, group, topic, threads, processor,
		processor,
		loggerWithField)
	return worker, err
}

func (p *myProcessor) StartDaemon() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.logger.Info("test demon thread starting ")
	go func() {
		timer := time.NewTicker(60 * time.Second)
	outer:
		for {
			select {
			case <-timer.C:

			case <-p.closeChan:
				break outer
			}
		}
		p.logger.Infof("test daemon goroutine exit")
	}()
	return nil
}

func (p *myProcessor) StopDaemon() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	close(p.closeChan)
	p.logger.Info("test daemon thread stopping ")
	return nil
}
