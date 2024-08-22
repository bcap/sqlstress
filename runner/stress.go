package runner

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"time"

	_ "github.com/go-sql-driver/mysql" // MySQL, driver: mysql
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL, driver: pgx
	_ "github.com/lib/pq"              // PostgreSQL, driver: postgres

	"github.com/bcap/sqlstress/config"
	"github.com/bcap/sqlstress/log"
)

func (r *Runner) Stress(ctx context.Context) error {
	ticker := time.NewTicker(time.Duration(r.config.CheckEverySeconds * float64(time.Second)))
	defer ticker.Stop()

	samplesToCollect := float64(r.config.AverageSamples)
	collectedSamples := 0.0
	growthFactor := r.config.GrowthFactor
	maxAdjustment := r.config.MaxConnectionDelta
	averages := make([]float64, len(r.config.Queries))
	throughputs := make([]int32, len(r.config.Queries))
	latencies := make([]int64, len(r.config.Queries))
	adjustEvery := uint64(r.config.AdjustConnectionsEveryXChecks)

	var checks uint64

	for {
		// collect and reset current counters
		for idx := range r.config.Queries {
			throughputs[idx] = r.throughput[idx].Load()
			latencies[idx] = r.latency[idx].Load()
			r.throughput[idx].Store(0)
			r.latency[idx].Store(0)
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

			avgLatency := float64(latencies[idx]) / throughput
			logProgress(idx, query, ratePerSecond, ratePerGoroutine, avgLatency, goroutines, target)

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
	query := r.config.Queries[queryID]

	var db *sql.DB

	reconnect := func() error {
		if db != nil {
			r.activeConnections.Add(-1)
		}

		connCtx, connCancel := context.WithTimeout(ctx, query.ConnectTimeout)
		defer connCancel()

		newDb, err := r.openDB(connCtx, query.ConnectionConfig)

		if ctx.Err() != nil {
			// main context cancelled, silently return
			return nil
		}

		if connCtx.Err() != nil {
			// timeout check
			return fmt.Errorf("timed out (current timeout: %v)", query.ConnectTimeout)
		}

		if err != nil {
			// regular error check
			return err
		}

		if db != nil {
			db.Close()
		}
		db = newDb
		r.activeConnections.Add(1)
		log.Debugf("query runner #%d-%d connected", queryID, id)
		return nil
	}

	splayRandom := rand.New(rand.NewSource(time.Now().UnixMicro()))

	keepTryingReconnect := func() {
		for {
			err := reconnect()
			if err == nil {
				return
			}

			sleep := r.config.RebuildConnWait
			if r.config.RebuildConnSplay > 0 {
				materializedSplay := splayRandom.Int63n(r.config.RebuildConnSplay.Nanoseconds())
				sleep += time.Duration(materializedSplay)
			}

			log.Errorf(
				"Query runner #%d-%d got an error while connecting: %v. Rebuilding connection in %v",
				queryID, id, err, sleep.Truncate(time.Millisecond),
			)

			select {
			case <-time.After(sleep):
			case <-ctx.Done():
				return
			}
		}
	}

	keepTryingReconnect()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	defer func() {
		if db != nil {
			r.activeConnections.Add(-1)
			db.Close()
		}
		log.Debugf("query runner #%d-%d stopped", queryID, id)
	}()

	randomWeights := query.Variables.RandomWeights()

	queryRandomC := make(chan int64, 10)
	go func() {
		defer close(queryRandomC)
		random := rand.New(rand.NewSource(query.RandomSeed))
		for {
			select {
			case queryRandomC <- random.Int63():
			case <-ctx.Done():
				return
			}
		}
	}()

	runCommand := func(command string) {
		command = materializeCommand(command, query.Variables, randomWeights, queryRandomC)

		start := time.Now()

		queryCtx := ctx
		var queryCtxCancel context.CancelFunc
		if query.ReadTimeout > 0 {
			queryCtx, queryCtxCancel = context.WithTimeout(ctx, query.ReadTimeout)
		}
		defer queryCtxCancel()

		onFailure := func(err error, action string) bool {
			if err == nil {
				return false
			}

			if ctx.Err() != nil {
				// silently abort in case of main context being cancelled
				return true
			}

			if queryCtx.Err() != nil {
				// timeout check
				log.Warnf(
					"query runner #%d-%d timed out while %s (current timeout: %v). Rebuilding connection",
					queryID, id, action, query.ReadTimeout,
				)
			} else {
				// other errors
				log.Warnf(
					"query runner #%d-%d got an error while %s: %v. Rebuilding connection",
					queryID, id, action, err,
				)
			}

			keepTryingReconnect()
			return true
		}

		result, err := db.QueryContext(queryCtx, command)
		if onFailure(err, "issuing query") {
			return
		}

		defer result.Close()

		// consume all records
		action := "reading query results"
		defer result.Close()
		_, err = result.Columns()
		if onFailure(err, action) {
			return
		}
		for {
			if result.NextResultSet() {
				for result.Next() {
					var throwAway any
					result.Scan(&throwAway)
				}
				if onFailure(result.Err(), action) {
					return
				}
			}
			if onFailure(result.Err(), action) {
				return
			} else {
				break
			}
		}
		r.latency[queryID].Add(time.Since(start).Nanoseconds())
		r.throughput[queryID].Add(1)
	}

	for {
		for _, command := range query.Commands {
			runCommand(command)
		}
		if query.Sleep > 0 {
			select {
			case <-time.After(query.Sleep):
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func logProgress(idx int, query *config.LoadQuery, actualRate float64, ratePerConnection float64, avgLatency float64, connections int, connectionTarget int) {
	// rate coloring
	desiredRate := query.Rate()
	actualRateS := fmt.Sprintf("%.1f", actualRate)
	at := actualRate / desiredRate
	atRed := 0.20    // display it in red if we are more than 20% off the target, above or below
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

	// name
	name := query.Name
	if len(name) > 10 {
		name = name[0:10]
	}

	log.Infof(
		"Query #%d %-10s: %s/s (%.1f/s), avg rate per connection: %0.1f/s, avg latency: %0.1fms, connections: %d (%+d%s)",
		idx, name, actualRateS, desiredRate, ratePerConnection, avgLatency/float64(time.Millisecond), connections, connectionTarget, limitMsg,
	)
}
