package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/alexflint/go-arg"
	"github.com/bcap/sqlstress/config"
	"github.com/bcap/sqlstress/runner"
)

type Args struct {
	Config        string `arg:"-c,--config"`
	ExampleConfig bool   `arg:"--example-config"`
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

	cfg := config.Default
	err := config.ParseFilePath(context.Background(), args.Config, &cfg)
	panicOnErr(err)

	rn := runner.New(cfg)
	err = rn.Run(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		panicOnErr(err)
	}
}

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}
