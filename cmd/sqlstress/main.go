package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/alexflint/go-arg"

	"github.com/bcap/sqlstress/config"
	"github.com/bcap/sqlstress/log"
	"github.com/bcap/sqlstress/runner"
)

type Args struct {
	Config        string `arg:"-c,--config"`
	ExampleConfig bool   `arg:"--example-config"`
	Verbose       bool   `arg:"-v,--verbose"`
}

func main() {
	var args Args
	arg.MustParse(&args)

	if args.ExampleConfig {
		config.GenExample(os.Stdout)
		os.Exit(0)
	} else if args.Config == "" {
		fmt.Fprintf(os.Stderr, "--config or --example-config need to be passed\n")
		os.Exit(1)
	}

	log.Level = log.InfoLevel
	if args.Verbose {
		log.Level = log.DebugLevel
	}

	cfg := config.Default
	err := config.ParseFilePath(context.Background(), args.Config, &cfg)
	panicOnErr(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		// We reset signal notifications and go back to default behavior
		// This means that a second signal will now kill the application
		signal.Reset()
		log.Warnf("Received %s signal, shutting down", strings.ToUpper(sig.String()))
		cancel()
	}()

	rn := runner.New(cfg)
	err = rn.Run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		panicOnErr(err)
	}
}

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}
