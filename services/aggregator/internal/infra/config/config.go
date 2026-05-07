package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	AWSRegion       string
	AWSEndpointURL  string
	InQueueName     string
	EventsTable     string
	SummaryTable    string
	Workers         int
	WaitTimeSeconds int32
	HTTPAddr        string
	LogLevel        string
}

func Load() (Config, error) {
	cfg := Config{
		AWSRegion:       getenv("AWS_REGION", "us-east-1"),
		AWSEndpointURL:  os.Getenv("AWS_ENDPOINT_URL"),
		InQueueName:     getenv("PROCESSED_EVENTS_QUEUE", "processed-events"),
		EventsTable:     getenv("EVENTS_TABLE", "events"),
		SummaryTable:    getenv("SUMMARY_TABLE", "developer_summary"),
		Workers:         envInt("AGGREGATOR_WORKERS", 5),
		WaitTimeSeconds: int32(envInt("SQS_WAIT_TIME_SECONDS", 10)),
		HTTPAddr:        getenv("HTTP_ADDR", ":8080"),
		LogLevel:        getenv("LOG_LEVEL", "info"),
	}
	if cfg.Workers < 1 {
		return Config{}, fmt.Errorf("AGGREGATOR_WORKERS must be >= 1")
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
