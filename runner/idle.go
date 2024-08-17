package runner

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/bcap/sqlstress/log"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

func (r *Runner) OpenIdle(ctx context.Context) error {
	totalIdle := 0
	for _, idle := range r.config.IdleConnections {
		if idle.Amount > 0 {
			totalIdle += idle.Amount
		}
	}
	if totalIdle == 0 {
		return nil
	}

	sem := semaphore.NewWeighted(int64(r.config.IdleConnMaxParallelism))
	group, gctx := errgroup.WithContext(ctx)

	log.Infof("Opening %d idle connections", totalIdle)

	conns := make([][]*sql.DB, len(r.config.IdleConnections))
	for idx1, idle := range r.config.IdleConnections {
		conns[idx1] = make([]*sql.DB, idle.Amount)
		for idx2 := 0; idx2 < idle.Amount; idx2++ {
			idle := idle
			idx1 := idx1
			idx2 := idx2
			group.Go(func() error {
				if err := sem.Acquire(gctx, 1); err != nil {
					return err
				}
				defer sem.Release(1)

				db, err := r.openDB(gctx, idle.ConnectionConfig)
				if err != nil {
					return err
				}
				conns[idx1][idx2] = db

				go r.keepAlive(ctx, db, fmt.Sprintf("%d-%d", idx1, idx2))
				return nil
			})
		}
	}
	if err := group.Wait(); err != nil {
		return err
	}
	log.Info("Idle connections done")
	r.idleConnections = conns
	return nil
}

func (r *Runner) CloseIdle() {
	var totalIdle int
	for _, conns := range r.idleConnections {
		totalIdle += len(conns)
	}
	log.Debugf("closing %d idle connections", totalIdle)
	for _, conns := range r.idleConnections {
		for _, conn := range conns {
			conn.Close()
		}
	}
}

func (r *Runner) keepAlive(ctx context.Context, conn *sql.DB, name string) {
	random := rand.New(rand.NewSource(time.Now().UnixMicro()))

	for {
		sleep := r.config.IdleConnKeepAlive
		if r.config.IdleConnKeepAliveSplay > 0 {
			sleep += time.Duration(random.Int63n(int64(r.config.IdleConnKeepAliveSplay)))
		}
		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			return
		}
		if err := conn.PingContext(ctx); err != nil {
			log.Errorf("idle connection %s died: %v", name, err)
			return
		}
		log.Debugf("idle connection %s alive", name)
	}
}
