package config

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v2"
)

const DefaultConnectTimeout = 5 * time.Second
const DefaultReadTimeout = 10 * time.Second

var Default = Config{
	AverageSamples:         5,
	GrowthFactor:           0.8,
	MaxConnectionDelta:     1,
	RebuildConnWait:        200 * time.Millisecond,
	RebuildConnSplay:       100 * time.Millisecond,
	IdleConnMaxParallelism: 10,
	IdleConnKeepAlive:      10 * time.Second,
	IdleConnKeepAliveSplay: 5 * time.Second,
}

type Config struct {
	Import []string `yaml:"import"`

	RunForSeconds     int64             `yaml:"run-for-seconds"`
	Driver            string            `yaml:"driver"`
	ConnectionStrings map[string]string `yaml:"connection-strings"`
	Setup             []Query           `yaml:"setup"`
	TearDown          []Query           `yaml:"teardown"`
	IdleConnections   []IdleConnection  `yaml:"idle-connections"`
	Queries           []*LoadQuery      `yaml:"queries"`

	// variables to tune the sql load mechanism
	CheckEverySeconds             float64 `yaml:"check-every-x-seconds"`
	AdjustConnectionsEveryXChecks int64   `yaml:"adjust-connections-on-every-x-checks"`
	AverageSamples                int     `yaml:"avg-samples"`
	GrowthFactor                  float64 `yaml:"growth-factor"`
	MaxConnectionDelta            int     `yaml:"max-connection-delta"`

	// other tuneables
	RebuildConnWait        time.Duration `yaml:"rebuild-connection-wait-time"`
	RebuildConnSplay       time.Duration `yaml:"rebuild-connection-splay-time"`
	IdleConnMaxParallelism int           `yaml:"idle-connection-max-parallelism"`
	IdleConnKeepAlive      time.Duration `yaml:"idle-connection-keep-alive-wait-time"`
	IdleConnKeepAliveSplay time.Duration `yaml:"idle-connection-keep-alive-splay-time"`
	ControlServer          bool          `yaml:"control-server"`
	ControlServerAddr      string        `yaml:"control-server-addr"`
}

type ConnectionConfig struct {
	Connection     string        `yaml:"connection"`
	ConnectTimeout time.Duration `yaml:"connect-timeout"`
}
type Query struct {
	ConnectionConfig `yaml:",inline"`

	ReadTimeout time.Duration `yaml:"read-timeout"`
	Commands    []string      `yaml:"commands"`
}
type LoadQuery struct {
	Query `yaml:",inline"`

	Name           string        `yaml:"name"`
	RatePerSecond  float64       `yaml:"rate-per-second"`
	RatePerMinute  float64       `yaml:"rate-per-minute"`
	MaxConnections int           `yaml:"max-connections"`
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

type IdleConnection struct {
	ConnectionConfig `yaml:",inline"`

	Amount int `yaml:"amount"`
}

func (c *Config) Print(w io.Writer) error {
	return yaml.NewEncoder(w).Encode(c)
}

func (c Config) Validate() error {
	if len(c.ConnectionStrings) == 0 {
		return fmt.Errorf("no connection strings defined")
	}

	for idx, setup := range c.Setup {
		if setup.Connection == "" && c.ConnectionStrings["default"] == "" {
			return fmt.Errorf("setup #%d has no connection reference and no \"default\" connection is defined in the connection strings", idx)
		}
		if setup.Connection != "" && c.ConnectionStrings[setup.Connection] == "" {
			return fmt.Errorf("setup #%d connection reference %q is not defined in the connection strings", idx, setup.Connection)
		}
	}

	for idx, tearDown := range c.TearDown {
		if tearDown.Connection == "" && c.ConnectionStrings["default"] == "" {
			return fmt.Errorf("tearDown #%d has no connection reference and no \"default\" connection is defined in the connection strings", idx)
		}
		if tearDown.Connection != "" && c.ConnectionStrings[tearDown.Connection] == "" {
			return fmt.Errorf("tearDown #%d connection reference %q is not defined in the connection strings", idx, tearDown.Connection)
		}
	}

	for idx, idle := range c.IdleConnections {
		if idle.Connection == "" && c.ConnectionStrings["default"] == "" {
			return fmt.Errorf("idle connection #%d has no connection reference and no \"default\" connection is defined in the connection strings", idx)
		}
		if idle.Connection != "" && c.ConnectionStrings[idle.Connection] == "" {
			return fmt.Errorf("idle connection #%d connection reference %q is not defined in the connection strings", idx, idle.Connection)
		}
	}

	for idx, query := range c.Queries {
		if err := query.Validate(c); err != nil {
			return fmt.Errorf("config query #%d is invalid: %w", idx, err)
		}
	}
	return nil
}

func (q *LoadQuery) Rate() float64 {
	if q.RatePerSecond > 0 {
		return q.RatePerSecond
	} else {
		return q.RatePerMinute * 60.0
	}
}

func (q Query) Validate(config Config) error {
	if q.Connection == "" {
		if config.ConnectionStrings["default"] == "" {
			return fmt.Errorf("query has no connection and no \"default\" connection is defined in the connection strings")
		}
	} else {
		if _, ok := config.ConnectionStrings[q.Connection]; !ok {
			return fmt.Errorf("query connection reference %q is not defined in the connection strings", q.Connection)
		}
	}
	return nil
}

func (q LoadQuery) Validate(config Config) error {
	if err := q.Query.Validate(config); err != nil {
		return err
	}

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

func ParseFilePath(ctx context.Context, filePath string, config *Config) error {
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	dir := filepath.Dir(filePath)
	return Parse(ctx, dir, f, config)
}

func Parse(ctx context.Context, dir string, reader io.Reader, config *Config) error {
	b, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	// parse and get import paths
	if err := parse(ctx, bytes.NewReader(b), config); err != nil {
		return err
	}

	// if there are imports
	if len(config.Import) > 0 {

		// we copy the imports and erase them to avoid recursion loops
		imports := make([]string, len(config.Import))
		copy(imports, config.Import)
		config.Import = nil

		// override config with all imported configs
		for _, importPath := range imports {
			if !filepath.IsAbs(importPath) {
				importPath = filepath.Join(dir, importPath)
			}

			if err := ParseFilePath(ctx, importPath, config); err != nil {
				return err
			}
		}

		// final parse run to override imported configs
		if err := parse(ctx, bytes.NewReader(b), config); err != nil {
			return err
		}
	}

	if err := config.Validate(); err != nil {
		return err
	}
	return nil
}

func parse(ctx context.Context, reader io.Reader, config *Config) error {
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

func GenExample(writer io.Writer) error {
	config := Config{
		Driver: "mysql",
		ConnectionStrings: map[string]string{
			"default": "user:password@tcp(localhost:3306)/database",
		},
		RunForSeconds: 1,
		Setup: []Query{
			{
				ConnectionConfig: ConnectionConfig{
					Connection: "default",
				},
				Commands: []string{
					"create table if not exists test (id int primary key)",
				},
			},
		},
		TearDown: []Query{
			{
				ConnectionConfig: ConnectionConfig{
					Connection: "default",
				},
				Commands: []string{
					"drop table if exists test",
				},
			},
		},
		Queries: []*LoadQuery{
			{
				RatePerSecond:  1,
				MaxConnections: 10,
				Query: Query{
					Commands: []string{
						"insert into test",
					},
				},
			},
			{
				RatePerSecond:  10,
				MaxConnections: 10,
				Query: Query{
					Commands: []string{
						"select 1",
						"select * from test",
					},
				},
			},
		},
	}
	encoder := yaml.NewEncoder(writer)
	return encoder.Encode(config)
}
