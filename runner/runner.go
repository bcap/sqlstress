package runner

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math"
	"sync/atomic"
	"time"

	"github.com/bcap/sqlstress/config"
	"github.com/fatih/color"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var yellow = color.New(color.FgYellow).SprintFunc()
var red = color.New(color.FgRed).SprintFunc()

type Runner struct {
	config     config.Config
	throughput []*atomic.Int32
	cancels    [][]context.CancelFunc
}

func New(config config.Config) *Runner {
	throughputs := make([]*atomic.Int32, len(config.Queries))
	for idx := range throughputs {
		throughputs[idx] = &atomic.Int32{}
	}
	return &Runner{
		config:     config,
		throughput: throughputs,
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

	log.Printf("This execution will run %s", durationMsg)

	log.Printf("Using the following configuration (full dump):")
	r.config.Print(log.Writer())
	log.Print("------")

	ticker := time.NewTicker(time.Duration(r.config.CheckEverySeconds * float64(time.Second)))
	defer ticker.Stop()

	samplesToCollect := float64(r.config.AverageSamples)
	collectedSamples := 0.0
	growthFactor := r.config.GrowthFactor
	maxAdjustment := r.config.MaxConnectionDelta
	averages := make([]float64, len(r.config.Queries))
	throughputs := make([]int32, len(r.config.Queries))

	for {
		// collect and reset current counters
		for idx := range r.config.Queries {
			throughputs[idx] = r.throughput[idx].Load()
			r.throughput[idx].Store(0)
		}

		// calculate averages and adjust runners
		for idx := range r.config.Queries {
			if collectedSamples < samplesToCollect {
				collectedSamples++
			}

			avg := averages[idx]
			throughput := float64(throughputs[idx])
			avg = avg - (avg / collectedSamples) + (throughput / collectedSamples)
			averages[idx] = avg

			ratePerSecond := avg / r.config.CheckEverySeconds
			query := r.config.Queries[idx]
			deltaRate := query.Rate() - ratePerSecond

			goroutinesAdjustment := 1.0
			ratePerGoroutine := 0.0
			goroutines := len(r.cancels[idx])
			if goroutines > 0 {
				ratePerGoroutine = ratePerSecond / float64(goroutines)
				goroutinesAdjustment = deltaRate / ratePerGoroutine * growthFactor
			}

			target := int(math.Round(goroutinesAdjustment))
			if target > maxAdjustment {
				target = maxAdjustment
			} else if target < -maxAdjustment {
				target = -maxAdjustment
			}

			// do not try to use more connections when we reach the max amount allowed
			if query.MaxConnections > 0 && target > 0 && goroutines+target > query.MaxConnections {
				target = query.MaxConnections - goroutines
			}

			logProgress(idx, query, ratePerSecond, ratePerGoroutine, goroutines, target)

			if target > 0 {
				for i := 0; i < target; i++ {
					childCtx, cancel := context.WithCancel(ctx)
					r.cancels[idx] = append(r.cancels[idx], cancel)
					go r.runQuery(childCtx, len(r.cancels[idx]), idx)
				}
			} else if target < 0 {
				for i := 0; i < -target; i++ {
					lastIdx := len(r.cancels[idx]) - 1
					cancel := r.cancels[idx][lastIdx]
					cancel()
					r.cancels[idx] = r.cancels[idx][0:lastIdx]
				}
			}
		}

		log.Print("------")

		// wait next tick
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *Runner) runQuery(ctx context.Context, id int, queryID int) error {
	// log.Printf("starting query runner #%d-%d", queryID, id)

	db, err := openDB(ctx, r.config.Driver, r.config.DSN)
	if err != nil {
		return err
	}

	defer func() {
		// log.Printf("query runner #%d-%d stopped", queryID, id)
		db.Close()
	}()

	query := r.config.Queries[queryID]

	for {
		for _, command := range query.Commands {
			result, err := db.QueryContext(ctx, command)
			if err == context.Canceled {
				return nil
			} else if err != nil {
				log.Printf("! query runner #%d-%d got an error while issuing a query: %v", queryID, id, err)
				return err
			}

			// consume all records
			if _, err := result.Columns(); err != nil {
				log.Printf("! query runner #%d-%d got an error while reading query results: %v", queryID, id, err)
			}
			for result.Next() {
			}
			result.Close()
			r.throughput[queryID].Add(1)

			if query.Sleep > 0 {
				select {
				case <-time.After(query.Sleep):
				case <-ctx.Done():
					return nil
				}
			}
		}
	}
}

func openDB(ctx context.Context, driver string, dsn string) (*sql.DB, error) {
	var db *sql.DB
	var err error

	doneC := make(chan struct{})
	go func() {
		db, err = sql.Open(driver, dsn)
		close(doneC)
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-doneC:
		return db, err
	}
}

func logProgress(idx int, query config.Query, actualRate float64, ratePerConnection float64, connections int, connectionTarget int) {
	// rate coloring
	desiredRate := query.Rate()
	actualRateS := fmt.Sprintf("%.1f", actualRate)
	at := actualRate / desiredRate
	atRed := 0.2     // display it in red if we are more than 20% off the target, above or below
	atYellow := 0.05 // display it in yellow if we are more than 5% off the target, above or below
	if at < 1-atRed || at > 1+atRed {
		actualRateS = red(actualRateS)
	} else if at < 1-atYellow || at > 1+atYellow {
		actualRateS = yellow(actualRateS)
	}

	// reached connections limit?
	limitMsg := ""
	if connections == query.MaxConnections {
		limitMsg = ", " + red("reached max")
	}

	log.Printf(
		"Query #%d: %s/s (%.1f/s), avg rate per connection: %0.1f/s, connections: %d (%+d%s), ",
		idx, actualRateS, desiredRate, ratePerConnection, connections, connectionTarget, limitMsg,
	)
}
