package config

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"gopkg.in/yaml.v2"
)

var Default = Config{
	AverageSamples:     5,
	GrowthFactor:       0.8,
	MaxConnectionDelta: 1,
}

type Config struct {
	Driver             string  `yaml:"driver"`
	DSN                string  `yaml:"dsn"`
	RunForSeconds      int64   `yaml:"run-for-seconds"`
	Queries            []Query `yaml:"queries"`
	CheckEverySeconds  float64 `yaml:"check-every-seconds"`
	AverageSamples     int     `yaml:"avg-samples"`
	GrowthFactor       float64 `yaml:"growth-factor"`
	MaxConnectionDelta int     `yaml:"max-connection-delta"`
}

type Query struct {
	RatePerSecond  float64       `yaml:"rate-per-second"`
	RatePerMinute  float64       `yaml:"rate-per-minute"`
	MaxConnections int           `yaml:"max-connections"`
	Commands       []string      `yaml:"commands"`
	Sleep          time.Duration `yaml:"sleep"`
}

func (c *Config) Print(w io.Writer) error {
	return yaml.NewEncoder(w).Encode(c)
}

func (c Config) Validate() error {
	for idx, query := range c.Queries {
		if err := query.Validate(); err != nil {
			return fmt.Errorf("config query #%d is invalid: %w", idx, err)
		}
	}
	return nil
}

func (q *Query) Rate() float64 {
	if q.RatePerSecond > 0 {
		return q.RatePerSecond
	} else {
		return q.RatePerMinute * 60.0
	}
}

func (q Query) Validate() error {
	if q.RatePerMinute > 0 && q.RatePerSecond > 0 {
		return fmt.Errorf("cannot specify both rate per second and rate per minute")
	}

	if q.RatePerMinute <= 0 && q.RatePerSecond <= 0 {
		return fmt.Errorf("at least one of rate per second or rate per minute need to be defined")
	}

	if len(q.Commands) == 0 {
		return fmt.Errorf("query has no commands")
	}

	for idx, command := range q.Commands {
		if command == "" {
			return fmt.Errorf("query command #%d is empty", idx)
		}
	}

	return nil
}

func Parse(ctx context.Context, reader io.Reader, config *Config) error {
	var err error

	doneC := make(chan struct{})

	go func() {
		decoder := yaml.NewDecoder(reader)
		err = decoder.Decode(config)
		close(doneC)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-doneC:
		return err
	}
}

func ParseFilePath(ctx context.Context, filePath string, config *Config) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	return Parse(ctx, f, config)
}

func GenExample(writer io.Writer) error {
	config := Config{
		Driver:        "mysql",
		DSN:           "user:password@tcp(localhost:3306)/database",
		RunForSeconds: 1,
		Queries: []Query{
			{
				RatePerSecond:  1,
				MaxConnections: 10,
				Commands: []string{
					"select 1",
					"select 2",
				},
			},
		},
	}
	encoder := yaml.NewEncoder(writer)
	return encoder.Encode(config)
}
