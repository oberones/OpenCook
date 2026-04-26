package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"

	"github.com/oberones/OpenCook/internal/admin"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/version"
)

func TestCommandDefaultsToServe(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)

	var loaded bool
	var served bool
	cmd.load = func() (config.Config, error) {
		loaded = true
		return config.Config{ListenAddress: ":4999"}, nil
	}
	cmd.runServer = func(_ context.Context, cfg config.Config, logger *log.Logger, build version.Info) error {
		served = true
		if cfg.ListenAddress != ":4999" {
			t.Fatalf("ListenAddress = %q, want :4999", cfg.ListenAddress)
		}
		if logger.Writer() != stdout {
			t.Fatal("server logger did not use command stdout")
		}
		if build.Version != "test-version" {
			t.Fatalf("build version = %q, want test-version", build.Version)
		}
		return nil
	}

	if code := cmd.Run(context.Background(), nil); code != exitOK {
		t.Fatalf("Run(no args) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	if !loaded || !served {
		t.Fatalf("loaded/served = %t/%t, want true/true", loaded, served)
	}
}

func TestCommandServeAliasUsesServerPath(t *testing.T) {
	cmd, _, stderr := newTestCommand(t)

	var served bool
	cmd.load = func() (config.Config, error) {
		return config.Config{ListenAddress: ":4000"}, nil
	}
	cmd.runServer = func(context.Context, config.Config, *log.Logger, version.Info) error {
		served = true
		return nil
	}

	if code := cmd.Run(context.Background(), []string{"serve"}); code != exitOK {
		t.Fatalf("Run(serve) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	if !served {
		t.Fatal("serve alias did not invoke server path")
	}
}

func TestCommandHelpAndVersionDoNotLoadServerConfig(t *testing.T) {
	for _, args := range [][]string{
		{"help"},
		{"--help"},
		{"-h"},
		{"serve", "--help"},
		{"admin", "--help"},
		{"admin", "help"},
		{"version"},
		{"--version"},
		{"-v"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			cmd, stdout, stderr := newTestCommand(t)
			cmd.load = func() (config.Config, error) {
				t.Fatalf("load config called for args %v", args)
				return config.Config{}, nil
			}
			cmd.runServer = func(context.Context, config.Config, *log.Logger, version.Info) error {
				t.Fatalf("run server called for args %v", args)
				return nil
			}

			if code := cmd.Run(context.Background(), args); code != exitOK {
				t.Fatalf("Run(%v) exit = %d, want %d; stderr = %s", args, code, exitOK, stderr.String())
			}
			if stdout.Len() == 0 {
				t.Fatalf("Run(%v) wrote no stdout", args)
			}
		})
	}
}

func TestCommandUsageErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "unknown command", args: []string{"bogus"}, want: `unknown command "bogus"`},
		{name: "serve extra arg", args: []string{"serve", "--bogus"}, want: "serve does not accept arguments"},
		{name: "help extra arg", args: []string{"help", "serve"}, want: "help does not accept arguments"},
		{name: "version extra arg", args: []string{"version", "--json"}, want: "version does not accept arguments"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd, _, stderr := newTestCommand(t)
			cmd.load = func() (config.Config, error) {
				t.Fatalf("load config called for args %v", tc.args)
				return config.Config{}, nil
			}
			cmd.runServer = func(context.Context, config.Config, *log.Logger, version.Info) error {
				t.Fatalf("run server called for args %v", tc.args)
				return nil
			}

			if code := cmd.Run(context.Background(), tc.args); code != exitUsage {
				t.Fatalf("Run(%v) exit = %d, want %d", tc.args, code, exitUsage)
			}
			if got := stderr.String(); !strings.Contains(got, tc.want) {
				t.Fatalf("stderr = %q, want to contain %q", got, tc.want)
			}
		})
	}
}

func TestCommandServerErrorsUseDependencyExit(t *testing.T) {
	for _, tc := range []struct {
		name      string
		load      func() (config.Config, error)
		runServer func(context.Context, config.Config, *log.Logger, version.Info) error
		want      string
	}{
		{
			name: "load config",
			load: func() (config.Config, error) {
				return config.Config{}, errors.New("bad config")
			},
			runServer: func(context.Context, config.Config, *log.Logger, version.Info) error {
				t.Fatal("run server called after config load failure")
				return nil
			},
			want: "load config: bad config",
		},
		{
			name: "run server",
			load: func() (config.Config, error) {
				return config.Config{ListenAddress: ":4000"}, nil
			},
			runServer: func(context.Context, config.Config, *log.Logger, version.Info) error {
				return errors.New("listen failed")
			},
			want: "run server: listen failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd, _, stderr := newTestCommand(t)
			cmd.load = tc.load
			cmd.runServer = tc.runServer

			if code := cmd.Run(context.Background(), nil); code != exitDependencyUnavailable {
				t.Fatalf("Run(no args) exit = %d, want %d", code, exitDependencyUnavailable)
			}
			if got := stderr.String(); !strings.Contains(got, tc.want) {
				t.Fatalf("stderr = %q, want to contain %q", got, tc.want)
			}
		})
	}
}

func newTestCommand(t *testing.T) (*command, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd := &command{
		stdout: stdout,
		stderr: stderr,
		build: version.Info{
			Version: "test-version",
			Commit:  "test-commit",
			BuiltAt: "test-built-at",
		},
		load: func() (config.Config, error) {
			t.Fatal("unexpected config load")
			return config.Config{}, nil
		},
		loadAdminConfig: func() admin.Config {
			t.Fatal("unexpected admin config load")
			return admin.Config{}
		},
		loadOffline: func() (config.Config, error) {
			t.Fatal("unexpected offline config load")
			return config.Config{}, nil
		},
		newAdmin: func(admin.Config) (adminJSONClient, error) {
			t.Fatal("unexpected admin client construction")
			return nil, nil
		},
		newOfflineStore: func(context.Context, string) (adminOfflineStore, func() error, error) {
			t.Fatal("unexpected offline store construction")
			return nil, nil, nil
		},
		newReindexTarget: func(string) (search.ReindexTarget, error) {
			t.Fatal("unexpected reindex target construction")
			return nil, nil
		},
		newSearchTarget: func(string) (search.ConsistencyTarget, error) {
			t.Fatal("unexpected search consistency target construction")
			return nil, nil
		},
		runServer: func(context.Context, config.Config, *log.Logger, version.Info) error {
			t.Fatal("unexpected server run")
			return nil
		},
	}
	return cmd, stdout, stderr
}
