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
	Driver                        string   `yaml:"driver"`
	DSN                           string   `yaml:"dsn"`
	RunForSeconds                 int64    `yaml:"run-for-seconds"`
	Setup                         []string `yaml:"setup"`
	TearDown                      []string `yaml:"tear-down"`
	Queries                       []Query  `yaml:"queries"`
	CheckEverySeconds             float64  `yaml:"check-every-x-seconds"`
	AdjustConnectionsEveryXChecks int64    `yaml:"adjust-connections-on-every-x-checks"`
	AverageSamples                int      `yaml:"avg-samples"`
	GrowthFactor                  float64  `yaml:"growth-factor"`
	MaxConnectionDelta            int      `yaml:"max-connection-delta"`
	IdleConnections               int      `yaml:"idle-connections"`
}

type Query struct {
	RatePerSecond  float64       `yaml:"rate-per-second"`
	RatePerMinute  float64       `yaml:"rate-per-minute"`
	MaxConnections int           `yaml:"max-connections"`
	Commands       []string      `yaml:"commands"`
	Sleep          time.Duration `yaml:"sleep"`
	Variables      QueryVars     `yaml:"vars"`
	RandomSeed     int64         `yaml:"random-seed"`
}

type QueryVars []QueryVar

type QueryVar struct {
	Key    string          `yaml:"key"`
	Value  string          `yaml:"value"`
	Values []QueryVarValue `yaml:"values"`
}

type QueryVarValue struct {
	Value  string `yaml:"value"`
	Weight int    `yaml:"weight"`
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

	seen := map[string]struct{}{}
	for idx, variable := range q.Variables {
		if variable.Key == "" {
			return fmt.Errorf("query variable #%d has no key", idx)
		}
		if _, ok := seen[variable.Key]; ok {
			return fmt.Errorf("query variable #%d uses key %s which is already defined", idx, variable.Key)
		}
		seen[variable.Key] = struct{}{}

		if variable.Value != "" && len(variable.Values) > 0 {
			return fmt.Errorf("query variable #%d specified voth \"value\" and \"values\", but only one of them is allowed", idx)
		}

		for vIdx, value := range variable.Values {
			if value.Weight < 0 {
				return fmt.Errorf("query variable #%d value #%d has invalid weight of %d", idx, vIdx, value.Weight)
			}
		}
	}

	return nil
}

func (v QueryVars) RandomWeights() [][]int {
	result := make([][]int, len(v))
	for idx, qVar := range v {
		result[idx] = qVar.RandomWeights()
	}
	return result
}

func (v QueryVar) RandomWeights() []int {
	if v.Value != "" {
		return nil
	}
	result := make([]int, 0, len(v.Values))
	for idx, value := range v.Values {
		weight := value.Weight
		if weight == 0 {
			weight = 1
		}
		for i := 0; i < weight; i++ {
			result = append(result, idx)
		}
	}
	return result
}

func Parse(ctx context.Context, reader io.Reader, config *Config) error {
	var err error

	doneC := make(chan struct{})

	go func() {
		err = yaml.NewDecoder(reader).Decode(config)
		close(doneC)
	}()

	select {
	case <-doneC:
		return err
	case <-ctx.Done():
		return ctx.Err()
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
