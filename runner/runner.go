package runner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/fatih/color"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/bcap/sqlstress/config"
	"github.com/bcap/sqlstress/log"
)

var yellow = color.New(color.FgYellow).SprintFunc()
var red = color.New(color.FgRed).SprintFunc()

type Runner struct {
	config            config.Config
	throughput        []*atomic.Int32
	cancels           [][]context.CancelFunc
	activeConnections atomic.Int32
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

	log.Infof("This execution will run %s", durationMsg)

	log.Infof("Using the following configuration (full dump):")
	if log.Level >= log.InfoLevel {
		r.config.Print(log.InfoLogger.Writer())
	}
	log.Infof("------")

	ticker := time.NewTicker(time.Duration(r.config.CheckEverySeconds * float64(time.Second)))
	defer ticker.Stop()

	samplesToCollect := float64(r.config.AverageSamples)
	collectedSamples := 0.0
	growthFactor := r.config.GrowthFactor
	maxAdjustment := r.config.MaxConnectionDelta
	averages := make([]float64, len(r.config.Queries))
	throughputs := make([]int32, len(r.config.Queries))
	adjustEvery := uint64(r.config.AdjustConnectionsEveryXChecks)

	var checks uint64

	for {
		// collect and reset current counters
		for idx := range r.config.Queries {
			throughputs[idx] = r.throughput[idx].Load()
			r.throughput[idx].Store(0)
		}

		// should this loop iteration adjust number of connections or not
		shouldAdjust := r.config.AdjustConnectionsEveryXChecks == 0 || checks&adjustEvery == 0

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
				if r.activeConnections.Load() > 0 {
					ratePerGoroutine = ratePerSecond / float64(goroutines)
					goroutinesAdjustment = deltaRate / ratePerGoroutine * growthFactor
				} else {
					// If we have goroutines up but no connections working, we may be having a temporary issue with the db
					// Avoid trying to keep increasing number of goroutines in this case
					goroutinesAdjustment = 0
				}
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

			// // do not try to adjust back to negative amount of connections
			if target < goroutines {
				target = -(goroutines - 1)
			}

			logProgress(idx, query, ratePerSecond, ratePerGoroutine, goroutines, target)

			if shouldAdjust {
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

		}

		checks++

		log.Infof("------")

		// wait next tick
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *Runner) runQuery(ctx context.Context, id int, queryID int) error {
	db, err := openDB(ctx, r.config.Driver, r.config.DSN)
	if err != nil {
		return err
	}

	connected := true
	r.activeConnections.Add(1)
	log.Debugf("query runner #%d-%d connected", queryID, id)

	defer func() {
		r.activeConnections.Add(-1)
		db.Close()
		log.Debugf("query runner #%d-%d stopped", queryID, id)
	}()

	rebuildConn := func(sleepFor time.Duration) {
		if connected {
			r.activeConnections.Add(-1)
			connected = false
		}
		select {
		case <-time.After(sleepFor):
		case <-ctx.Done():
			return
		}
		newDb, err := openDB(ctx, r.config.Driver, r.config.DSN)
		if err == nil {
			db.Close()
			db = newDb
			connected = true
			r.activeConnections.Add(1)
			log.Debugf("query runner #%d-%d re-connected", queryID, id)
		}
	}

	query := r.config.Queries[queryID]
	randomWeights := query.Variables.RandomWeights()

	randomC := make(chan int64, 10)
	go func() {
		defer close(randomC)
		random := rand.New(rand.NewSource(query.RandomSeed))
		for {
			select {
			case randomC <- random.Int63():
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		for _, command := range query.Commands {
			command = materializeCommand(command, query.Variables, randomWeights, randomC)

			result, err := db.QueryContext(ctx, command)
			if errors.Is(err, context.Canceled) {
				return nil
			} else if err != nil {
				log.Warnf("query runner #%d-%d got an error while issuing a query: %v. Rebuilding connection in 200ms", queryID, id, err)
				rebuildConn(200 * time.Millisecond)
				continue
			}

			// consume all records
			if _, err := result.Columns(); err != nil {
				log.Warnf("query runner #%d-%d got an error while reading query results: %v. Rebuilding connection in 200ms", queryID, id, err)
				rebuildConn(200 * time.Millisecond)
				continue
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

	log.Infof(
		"Query #%d: %s/s (%.1f/s), avg rate per connection: %0.1f/s, connections: %d (%+d%s), ",
		idx, actualRateS, desiredRate, ratePerConnection, connections, connectionTarget, limitMsg,
	)
}
