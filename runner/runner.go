package runner

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/fatih/color"
	_ "github.com/go-sql-driver/mysql" // MySQL, driver: mysql
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL, driver: pgx
	_ "github.com/lib/pq"              // PostgreSQL, driver: postgres

	"github.com/bcap/sqlstress/config"
	"github.com/bcap/sqlstress/log"
)

var yellow = color.New(color.FgYellow).SprintFunc()
var red = color.New(color.FgRed).SprintFunc()

type Runner struct {
	config            config.Config
	throughput        []*atomic.Int32
	latency           []*atomic.Int64
	cancels           [][]context.CancelFunc
	activeConnections atomic.Int32
	idleConnections   [][]*sql.DB
}

func New(config config.Config) *Runner {
	throughputs := make([]*atomic.Int32, len(config.Queries))
	latency := make([]*atomic.Int64, len(config.Queries))
	for idx := range throughputs {
		throughputs[idx] = &atomic.Int32{}
		latency[idx] = &atomic.Int64{}
	}
	return &Runner{
		config:     config,
		throughput: throughputs,
		latency:    latency,
		cancels:    make([][]context.CancelFunc, len(config.Queries)),
	}
}

func (r *Runner) Run(ctx context.Context) error {
	durationMsg := "until interrupted"
	if r.config.RunForSeconds > 0 {
		duration := time.Duration(r.config.RunForSeconds) * time.Second
		durationMsg = fmt.Sprintf("for %v", duration)
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, duration)
		defer cancel()
	}

	log.Infof("This execution will run %s", durationMsg)

	log.Infof("Using the following configuration (full dump):")
	if log.Level >= log.InfoLevel {
		r.config.Print(log.InfoLogger.Writer())
	}

	log.Infof("------")

	// defer Teardown
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()
		r.TearDown(ctx)
	}()

	// Do the setup
	if err := r.Setup(ctx); err != nil {
		return err
	}

	// Open idle connections
	if err := r.OpenIdle(ctx); err != nil {
		return err
	}
	defer r.CloseIdle()

	log.Infof("------")

	// Stress the DB(s)
	return r.Stress(ctx)
}
