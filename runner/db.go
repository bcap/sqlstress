package runner

import (
	"context"
	"database/sql"
	"time"

	"github.com/bcap/sqlstress/config"
)

func (r *Runner) openDB(ctx context.Context, connCfg config.ConnectionConfig) (*sql.DB, error) {
	timeout := config.DefaultConnectTimeout
	if connCfg.ConnectTimeout > 0 {
		timeout = connCfg.ConnectTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	connStr := r.config.ConnectionStrings["default"]
	if connCfg.Connection != "" {
		connStr = r.config.ConnectionStrings[connCfg.Connection]
	}

	return openDB(ctx, r.config.Driver, connStr)
}

func openDB(ctx context.Context, driver string, dsn string) (*sql.DB, error) {
	var db *sql.DB
	var err error

	doneC := make(chan struct{})
	go func() {
		defer close(doneC)
		db, err = sql.Open(driver, dsn)
		if err != nil {
			return
		}
		err = db.PingContext(ctx)
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-doneC:
		return db, err
	}
}

func dbExec(ctx context.Context, db *sql.DB, command string, readTimeout time.Duration) (sql.Result, error) {
	if readTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, readTimeout)
		defer cancel()
	}
	return db.ExecContext(ctx, command)
}
