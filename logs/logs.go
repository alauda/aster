package logs

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/alauda/aster/utils"
	rotatelogs "github.com/lestrrat/go-file-rotatelogs"
	"io"
	"os"
)

type LogConfig struct {
	Stdout    bool
	LogDir    string
	AppName   string
	Level     logrus.Level
	Formatter logrus.Formatter
}

func DefaultTextFormatter() logrus.Formatter {
	customFormatter := new(logrus.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05.000"
	customFormatter.FullTimestamp = true
	return customFormatter
}

func InitStdoutLogger() *logrus.Logger {
	customFormatter := DefaultTextFormatter()
	logger := logrus.New()
	logger.Out = os.Stdout
	logger.Level = logrus.DebugLevel
	logger.Formatter = customFormatter
	return logger
}

func SetupLogger(conf LogConfig) (*logrus.Logger, error) {

	if conf.Stdout {
		return InitStdoutLogger(), nil
	}

	var err error
	path := fmt.Sprintf("%s/%s.log.%s", conf.LogDir, conf.AppName, "%Y%m%d")
	fmt.Println(path)
	if err = utils.EnsureDir(path); err != nil {
		return nil, err
	}

	rl, err := rotatelogs.New(path)
	if err != nil {
		return nil, err
	}
	rotatelogs.WithLinkName(fmt.Sprintf("%s/%s", conf.LogDir, conf.AppName)).Configure(rl)

	out := io.MultiWriter(os.Stdout, rl)
	logger := logrus.Logger{
		//Formatter: &logrus.JSONFormatter{},
		Formatter: conf.Formatter,
		Level:     conf.Level,
		Out:       out,
	}
	logger.Info("Setup log finished.")

	return &logger, nil
}
