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

	SchedulerId      string        `envconfig:"SCHEDULER_ID" default:""`
	SchTickInterval  time.Duration `envconfig:"SCHEDULER_TICK_INTERVAL" default:"5s"`
	SchDispatchCount int           `envconfig:"SCHEDULER_DISPATCH_COUNT" default:"100"`

	SchLeaseDuration        time.Duration `envconfig:"SCHEDULER_LEASE_DURATION" default:"30s"`
	SchLeaseRenewalInterval time.Duration `envconfig:"SCHEDULER_LEASE_RENEWAL_INTERVAL" default:"10s"`
	SchLeaseRecoveryTick    time.Duration `envconfig:"SCHEDULER_LEASE_RECOVERY_TICK" default:"30s"`

	WkrPoolSize int `envconfig:"WORKER_POOL_SIZE" default:"6"`
}

func Load() (Config, error) {
	var config Config
	err := envconfig.Process("", &config)
	return config, err
}
