package main

import (
	"log"
	"os"
	"strconv"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {
	OutQueueName string // SQS queue name for outbound documents

	MessageBucketName string // the bucket to use for large messages

	DataSourceName string // the name to associate the data with. Each record has metadata showing this value
	FileName       string // the input file name
	MaxCount       uint   // the maximum document count to ingest, 0 is no limit

	WorkerQueueSize int // the inbound message queue size to feed the workers
	Workers         int // the number of worker processes
}

func ensureSet(env string) string {
	val, set := os.LookupEnv(env)

	if set == false {
		log.Printf("environment variable not set: [%s]", env)
		os.Exit(1)
	}

	return val
}

func ensureSetAndNonEmpty(env string) string {
	val := ensureSet(env)

	if val == "" {
		log.Printf("environment variable not set: [%s]", env)
		os.Exit(1)
	}

	return val
}

func envToInt(env string) int {

	number := ensureSetAndNonEmpty(env)
	n, err := strconv.Atoi(number)
	fatalIfError(err)
	return n
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	var cfg ServiceConfig

	cfg.OutQueueName = ensureSetAndNonEmpty("VIRGO4_SIMPLE_INGEST_OUT_QUEUE")
	cfg.MessageBucketName = ensureSetAndNonEmpty("VIRGO4_SQS_MESSAGE_BUCKET")
	cfg.DataSourceName = ensureSetAndNonEmpty("VIRGO4_SIMPLE_INGEST_DATA_SOURCE")
	cfg.MaxCount = uint(envToInt("VIRGO4_SIMPLE_INGEST_MAX_COUNT"))
	cfg.FileName = ensureSetAndNonEmpty("VIRGO4_SIMPLE_INGEST_FILE_NAME")
	cfg.WorkerQueueSize = envToInt("VIRGO4_SIMPLE_INGEST_WORK_QUEUE_SIZE")
	cfg.Workers = envToInt("VIRGO4_SIMPLE_INGEST_WORKERS")

	log.Printf("[CONFIG] OutQueueName         = [%s]", cfg.OutQueueName)
	log.Printf("[CONFIG] DataSourceName       = [%s]", cfg.DataSourceName)
	log.Printf("[CONFIG] MessageBucketName    = [%s]", cfg.MessageBucketName)
	log.Printf("[CONFIG] FileName             = [%s]", cfg.FileName)
	log.Printf("[CONFIG] MaxCount             = [%d]", cfg.MaxCount)
	log.Printf("[CONFIG] WorkerQueueSize      = [%d]", cfg.WorkerQueueSize)
	log.Printf("[CONFIG] Workers              = [%d]", cfg.Workers)

	return &cfg
}
