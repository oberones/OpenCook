package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/oberones/OpenCook/internal/app"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/version"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	logger := log.New(os.Stdout, "", log.LstdFlags|log.LUTC)

	application, err := app.New(cfg, logger, version.Current())
	if err != nil {
		logger.Fatalf("build application: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := application.Run(ctx); err != nil {
		logger.Fatalf("run application: %v", err)
	}
}
