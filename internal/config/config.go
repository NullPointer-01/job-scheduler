package config

import "github.com/kelseyhightower/envconfig"

type Config struct {
	ServerAddr string `envconfig:"SERVER_ADDR" default:"localhost"`
	ServerPort string `envconfig:"SERVER_PORT" default:"20000"`
}

func Load() (Config, error) {
	var config Config
	err := envconfig.Process("", &config)
	return config, err
}
