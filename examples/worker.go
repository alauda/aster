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
	"sync"
	"syscall"
	"time"
)

const (
	// Each consumer should has a name
	WORKER_NAME = "my-worker"
)

// Register this worker to aster at application startup automatically
func init() {
	worker.Register(WORKER_NAME, Init)
}

// Aster will use this func to get a woker instance,
// which will has more than one worker threads(go routines)
// This func should implemented by user
func Init() (*worker.ConsumerWorker, error) {

	brokers := utils.EnvString(common.ENV_KEY_BROKERS, "")
	if len(brokers) == 0 {
		return nil, fmt.Errorf("No brokers provided")
	}

	group := "test-group"
	topic := "test-topic"
	// Thie worker will has 4 threads run concurrently
	var threads int64 = 4

	logConf := logs.LogConfig{
		LogDir:    utils.EnvString(common.ENV_KEY_LOG_DIR, "/var/log/myworker"),
		AppName:   "test-consumer", // For logging
		Formatter: logs.DefaultTextFormatter(),
		Level:     logrus.DebugLevel,
	}

	var logger *logrus.Logger
	var err error
	logger, err = logs.SetupLogger(logConf)
	if err != nil {
		return nil, err
	}
	// Add tags for logging
	loggerWithField := logger.WithField("worker", "test-consumer")

	processor := &myProcessor{
		logger:    loggerWithField,
		mutex:     &sync.Mutex{},
		closeChan: make(chan bool),
		messages:  0,
	}
	worker, err := worker.NewConsumerWorker(brokers, group, topic, threads, processor,
		loggerWithField)
	return worker, err
}

// Main function as entry point of an application
func main() {

	// This two envs will be used by aster worker.
	os.Setenv(common.ENV_KEY_BROKERS, "localhost:9092")
	os.Setenv(common.ENV_KEY_WORKERS, WORKER_NAME)

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger, _ := logs.SetupLogger(logs.LogConfig{Stdout: true})

	// Use daemon.NewDaemon to create and run a daemon thread
	daemon := daemon.NewDaemon(logger)
	run := true

	if err := daemon.Start(); err != nil {
		logger.Fatalf("start daemon error: %s", err)
	}

	// Wait a signal to exit app
	for run {
		select {
		case sig := <-sigChan:
			logger.Infof("Caught signal %v: terminating", sig)
			run = false
		}
	}

	// Stop daemon
	daemon.Stop()
	// Wait daemon to exit
	daemon.Wait()
	logger.Info("app will exist!")
}

// User implement funcs
type myProcessor struct {
	logger    *logrus.Entry
	mutex     *sync.Mutex
	messages  int64
	closeChan chan bool
}

// First, this func will be called when new message received.
func (p *myProcessor) ProcessMessage(message *kafka.Message) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.logger.Debugf("Receive: %s", string(message.Value))
	p.messages++
	return nil
}

// This func will call at worker startup time.
func (p *myProcessor) StartDaemon() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.logger.Info("test demon thread starting ")
	go func() {
		timer := time.NewTicker(5 * time.Second)
	outer:
		for {
			select {
			case <-timer.C:
				p.logger.Infof("message processed: %d", p.messages)
			case <-p.closeChan:
				break outer
			}
		}
		p.logger.Infof("test daemon goroutine exit")
	}()
	return nil
}

// When worker received a stop signal, this func will be called
func (p *myProcessor) StopDaemon() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	close(p.closeChan)
	p.logger.Info("test daemon thread stopping ")
	return nil
}
