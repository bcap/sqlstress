package runner

import (
	"context"
	"strings"

	"github.com/bcap/sqlstress/config"
	"github.com/bcap/sqlstress/log"
)

func (r *Runner) Setup(ctx context.Context) error {
	return r.runPrePostQueries(ctx, r.config.Setup, "setup")
}

func (r *Runner) TearDown(ctx context.Context) error {
	return r.runPrePostQueries(ctx, r.config.TearDown, "tear down")
}

func (r *Runner) runPrePostQueries(ctx context.Context, queries []config.Query, action string) error {
	numCommands := 0
	for _, q := range queries {
		numCommands += len(q.Commands)
	}
	if numCommands == 0 {
		return nil
	}

	log.Infof("Running %d %s commands", numCommands, action)
	for idx1, query := range queries {
		db, err := r.openDB(ctx, query.ConnectionConfig)
		if err != nil {
			log.Errorf("Error opening db connection for running %s commands %d: %v", action, idx1, err)
			return err
		}
		defer db.Close()
		for idx2, command := range query.Commands {
			if _, err := dbExec(ctx, db, command, query.ReadTimeout); err != nil {
				log.Errorf("Error running %s command %d-%d: %v", action, idx1, idx2, err)
				return nil
			}
		}
	}
	log.Infof("%s done", strings.ToUpper(action[0:1])+action[1:])
	return nil
}
