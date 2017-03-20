package worker

import (
	"fmt"
	"github.com/Sirupsen/logrus"
)

var (
	workers map[string]InitFunc
	log     = logrus.WithFields(logrus.Fields{"pkg": "aster"})
)

type InitFunc func() (*ConsumerWorker, error)

func init() {
	workers = make(map[string]InitFunc)
}

func Register(workerName string, initFunc InitFunc) error {

	log.Debugf("Registering worker %s", workerName)

	if _, exists := workers[workerName]; exists {
		return fmt.Errorf("Worker %s has already been registered", workerName)
	}
	workers[workerName] = initFunc
	return nil
}

func GetConsumerWorker(workerName string) (*ConsumerWorker, error) {
	if _, exists := workers[workerName]; !exists {
		return nil, fmt.Errorf("ConsumerWorker %s is not supported!", workerName)
	}
	return workers[workerName]()
}
