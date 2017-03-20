package daemon

import (
	"errors"
	"github.com/Sirupsen/logrus"
	"github.com/alauda/aster/common"
	"github.com/alauda/aster/utils"
	"github.com/alauda/aster/worker"
	"strings"
	"sync"
)

type daemon struct {
	consumeWorkers map[string]*worker.ConsumerWorker
	waitGroup      *sync.WaitGroup
	logger         *logrus.Logger
}

func NewDaemon(logger *logrus.Logger) *daemon {
	return &daemon{
		consumeWorkers: make(map[string]*worker.ConsumerWorker),
		waitGroup:      &sync.WaitGroup{},
		logger:         logger,
	}
}

func (d *daemon) Wait() {
	d.logger.Info("begin wait daemon to stop")
	d.waitGroup.Wait()
	d.logger.Info("daemon stopped!")
}

func (d *daemon) Stop() {
	for name, worker := range d.consumeWorkers {
		d.logger.Infof("begin to stop consumer worker %s", name)
		worker.Stop()
		worker.Wait()
		d.logger.Infof("consumer worker %s stopped", name)
	}
	d.logger.Infof("all consumer worker stopped")
}

func (d *daemon) Start() error {
	workers := utils.EnvString(common.ENV_KEY_WORKERS, "")

	if len(workers) == 0 {
		return errors.New("No worker in env")
	}

	workerCount := 0
	workerList := strings.Split(workers, ",")
	for _, workerName := range workerList {
		if _, ok := d.consumeWorkers[workerName]; !ok {
			worker, err := worker.GetConsumerWorker(workerName)
			if err == nil {
				workerCount++
				d.consumeWorkers[workerName] = worker
			} else {
				d.logger.Errorf("error when create worker for %s, error: %s", workerName, err)
			}

		}
	}

	if workerCount == 0 {
		return errors.New("No worker created!")
	}

	for name, worker := range d.consumeWorkers {
		go func() {
			d.waitGroup.Add(1)
			d.logger.Infof("consumer worker %s started!", name)
			worker.Start()
			worker.Wait()
			d.logger.Infof("consumer worker %s end wait!", name)
			d.waitGroup.Done()
			d.logger.Infof("consumer worker %s stopped!", name)
		}()

	}

	d.logger.Info("daemon main started")
	return nil

}
