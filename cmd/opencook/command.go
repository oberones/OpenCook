package main

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/oberones/OpenCook/internal/admin"
	"github.com/oberones/OpenCook/internal/app"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/version"
)

const (
	exitOK                    = 0
	exitUsage                 = 1
	exitNotFound              = 2
	exitPartial               = 3
	exitDependencyUnavailable = 4
)

type command struct {
	stdout           io.Writer
	stderr           io.Writer
	build            version.Info
	load             func() (config.Config, error)
	loadAdminConfig  func() admin.Config
	loadOffline      func() (config.Config, error)
	newAdmin         func(admin.Config) (adminJSONClient, error)
	newOfflineStore  func(context.Context, string) (adminOfflineStore, func() error, error)
	newReindexTarget func(string) (search.ReindexTarget, error)
	newSearchTarget  func(string) (search.ConsistencyTarget, error)
	runServer        func(context.Context, config.Config, *log.Logger, version.Info) error
}

func newCommand(stdout, stderr io.Writer) *command {
	return &command{
		stdout:          stdout,
		stderr:          stderr,
		build:           version.Current(),
		load:            config.LoadFromEnv,
		loadAdminConfig: admin.LoadConfigFromEnv,
		loadOffline:     config.LoadFromEnv,
		newAdmin: func(cfg admin.Config) (adminJSONClient, error) {
			return admin.NewClient(cfg)
		},
		newOfflineStore: newPostgresAdminOfflineStore,
		newReindexTarget: func(raw string) (search.ReindexTarget, error) {
			return search.NewOpenSearchClient(raw)
		},
		newSearchTarget: func(raw string) (search.ConsistencyTarget, error) {
			return search.NewOpenSearchClient(raw)
		},
		runServer: runServer,
	}
}

func (c *command) Run(ctx context.Context, args []string) int {
	if len(args) == 0 {
		return c.runServe(ctx)
	}

	switch args[0] {
	case "admin":
		return c.runAdminCommand(ctx, args[1:])
	case "serve":
		return c.runServeCommand(ctx, args[1:])
	case "help", "-h", "--help":
		if len(args) > 1 {
			return c.usageError("%s does not accept arguments: %v\n\n", args[0], args[1:])
		}
		c.printUsage(c.stdout)
		return exitOK
	case "version", "-v", "--version":
		if len(args) > 1 {
			return c.usageError("%s does not accept arguments: %v\n\n", args[0], args[1:])
		}
		c.printVersion()
		return exitOK
	default:
		return c.usageError("unknown command %q\n\n", args[0])
	}
}

func (c *command) runServeCommand(ctx context.Context, args []string) int {
	if len(args) == 0 {
		return c.runServe(ctx)
	}
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		c.printServeUsage(c.stdout)
		return exitOK
	}

	fmt.Fprintf(c.stderr, "serve does not accept arguments: %v\n\n", args)
	c.printServeUsage(c.stderr)
	return exitUsage
}

func (c *command) usageError(format string, args ...any) int {
	fmt.Fprintf(c.stderr, format, args...)
	c.printUsage(c.stderr)
	return exitUsage
}

func (c *command) runServe(ctx context.Context) int {
	cfg, err := c.load()
	if err != nil {
		fmt.Fprintf(c.stderr, "load config: %v\n", err)
		return exitDependencyUnavailable
	}

	logger := log.New(c.stdout, "", log.LstdFlags|log.LUTC)
	if err := c.runServer(ctx, cfg, logger, c.build); err != nil {
		fmt.Fprintf(c.stderr, "run server: %v\n", err)
		return exitDependencyUnavailable
	}
	return exitOK
}

func (c *command) printUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  opencook [serve]
  opencook admin COMMAND
  opencook version
  opencook help

Commands:
  admin      Run operational administration commands against a live OpenCook server.
  serve      Start the OpenCook HTTP server. This is also the default with no arguments.
  version    Print build version information.
  help       Show this help.
`)
}

func (c *command) printServeUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  opencook serve

Start the OpenCook HTTP server using OPENCOOK_* environment configuration.
`)
}

func (c *command) printVersion() {
	fmt.Fprintf(c.stdout, "opencook %s\ncommit: %s\nbuilt_at: %s\n", c.build.Version, c.build.Commit, c.build.BuiltAt)
}

func runServer(ctx context.Context, cfg config.Config, logger *log.Logger, build version.Info) error {
	application, err := app.New(cfg, logger, build)
	if err != nil {
		return fmt.Errorf("build application: %w", err)
	}
	return application.Run(ctx)
}
