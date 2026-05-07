package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	AWSRegion       string
	AWSEndpointURL  string
	RawQueueName    string
	OutQueueName    string
	Workers         int
	ProcessorID     string
	WaitTimeSeconds int32
	LogLevel        string
}

func Load() (Config, error) {
	defaultID, _ := os.Hostname()
	if defaultID == "" {
		defaultID = "processor"
	}
	cfg := Config{
		AWSRegion:       getenv("AWS_REGION", "us-east-1"),
		AWSEndpointURL:  os.Getenv("AWS_ENDPOINT_URL"),
		RawQueueName:    getenv("RAW_EVENTS_QUEUE", "raw-events"),
		OutQueueName:    getenv("PROCESSED_EVENTS_QUEUE", "processed-events"),
		ProcessorID:     getenv("PROCESSOR_ID", defaultID),
		LogLevel:        getenv("LOG_LEVEL", "info"),
		Workers:         envInt("PROCESSOR_WORKERS", 5),
		WaitTimeSeconds: int32(envInt("SQS_WAIT_TIME_SECONDS", 10)),
	}
	if cfg.Workers < 1 {
		return Config{}, fmt.Errorf("PROCESSOR_WORKERS must be >= 1")
	}
	if cfg.WaitTimeSeconds < 0 || cfg.WaitTimeSeconds > 20 {
		return Config{}, fmt.Errorf("SQS_WAIT_TIME_SECONDS must be between 0 and 20")
	}
	return cfg, nil
}


func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
