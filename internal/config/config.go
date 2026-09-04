package config

import "github.com/kelseyhightower/envconfig"

type Config struct {
	ServerAddr  string `envconfig:"SERVER_ADDR" default:"localhost"`
	ServerPort  string `envconfig:"SERVER_PORT" default:"20000"`
	DatabaseURL string `envconfig:"DATABASE_URL" default:"postgresql://postgres@localhost:5432/mydb"`
}

func Load() (Config, error) {
	var config Config
	err := envconfig.Process("", &config)
	return config, err
}
