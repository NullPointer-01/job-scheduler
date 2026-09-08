package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	ServerAddr string `envconfig:"SERVER_ADDR" default:"localhost"`
	ServerPort string `envconfig:"SERVER_PORT" default:"20000"`

	MetricsAddr string `envconfig:"METRICS_ADDR"  default:"localhost"`
	MetricsPort string `envconfig:"METRICS_PORT" default:"30000"`
	DatabaseURL string `envconfig:"DATABASE_URL" default:"postgresql://postgres@localhost:5432/mydb"`

	SchTickInterval  time.Duration `envconfig:"SCHEDULER_TICK_INTERVAL" default:"3s"`
	SchDispatchCount int           `envconfig:"SCHEDULER_DISPATCH_COUNT" default:"100"`

	WkrPoolSize int `envconfig:"WORKER_POOL_SIZE" default:"6"`
}

func Load() (Config, error) {
	var config Config
	err := envconfig.Process("", &config)
	return config, err
}
