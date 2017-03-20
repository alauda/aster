package utils

import (
	"os"
	"path"
	"strconv"
	"time"
)

func EnvString(key, defaultValue string) string {
	v := os.Getenv(key)
	if len(v) == 0 {
		return defaultValue
	}
	return v
}

func EnvInt64(key string, defaultValue int64) int64 {
	v := os.Getenv(key)
	if len(v) == 0 {
		return defaultValue
	}
	if int64Value, err := strconv.ParseInt(v, 10, 64); err == nil {
		return int64Value
	}
	return defaultValue
}

func EnsureDir(p string) error {
	path := path.Dir(p)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, os.ModeDir|0700); err != nil {
			return err
		}
	}
	return nil
}

func TimestampMillisecond() int64 {
	return time.Now().UnixNano() / 1e6
}