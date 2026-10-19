package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"

	"github.com/Trifocals3537/tessarr/cmd/tessarr"
	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logsafe"
	"golang.org/x/term"
)

const defaultPprofAddress = "127.0.0.1:6060"

func main() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("FATAL: Recovered from panic in main: %s\n", logsafe.Text(fmt.Sprint(r)))
			debug.PrintStack()
		}
	}()
	if len(os.Args) > 1 && os.Args[1] == "migrate-from-decypharr" {
		if err := runLegacyMigration(os.Args[2:], os.Stdout, os.Stderr); err != nil {
			log.Fatalf("state migration failed: %s", logsafe.Text(err.Error()))
		}
		return
	}

	var configPath string
	var pprofAddr string
	var checkConfig bool
	var setAuthUsername string

	// Create a default config directory if it doesn't exist
	flag.StringVar(&configPath, "config", "", "path to the data folder")
	flag.StringVar(
		&pprofAddr,
		"pprof",
		defaultPprofAddress,
		"loopback pprof server address (set to empty to disable)",
	)
	flag.BoolVar(&checkConfig, "check-config", false, "validate configuration without starting services")
	flag.StringVar(
		&setAuthUsername,
		"set-auth",
		"",
		"securely set the web username and password, then exit",
	)
	flag.Parse()

	// get enable pprof flag from environment variable if not set via flag
	enablePprof := os.Getenv("ENABLE_PPROF") != ""

	if configPath == "" {
		defaultDir, err := os.UserHomeDir()
		if err != nil {
			// If we can't get the user home directory, fallback to current directory
			defaultDir = "."
		}
		defaultConfigDir := filepath.Join(defaultDir, ".tessarr")
		configPath = defaultConfigDir
	}

	if checkConfig && setAuthUsername != "" {
		log.Fatal("--check-config and --set-auth cannot be used together")
	}

	if checkConfig {
		cfg, err := config.LoadForValidation(configPath)
		if err != nil {
			log.Fatalf("Tessarr configuration check failed: %s", logsafe.Text(err.Error()))
		}
		if err := cfg.Validate(); err != nil {
			log.Fatalf("Tessarr configuration check failed: %s", logsafe.Text(err.Error()))
		}
		if err := cfg.ValidateDeployment(); err != nil {
			log.Fatalf("Tessarr deployment safety check failed: %s", logsafe.Text(err.Error()))
		}
		if len(cfg.AllowedClientCIDRs) == 0 &&
			!config.IsLoopbackBindAddress(cfg.BindAddress) {
			log.Printf(
				"WARNING: non-loopback HTTP listener has no allowed_client_cidrs boundary; use a trusted network or TLS proxy and do not send credentials to the listener over the public Internet",
			)
		}
		fmt.Printf(
			"Tessarr configuration is valid: %s\n",
			logsafe.Text(filepath.Join(configPath, "config.json")),
		)
		return
	}

	if err := config.SetConfigPath(configPath); err != nil {
		log.Fatal("Invalid Tessarr data path")
	}
	cfg := config.Get()

	if setAuthUsername != "" {
		if err := configureAuthFromTerminal(cfg, setAuthUsername); err != nil {
			log.Fatalf("Tessarr authentication setup failed: %s", logsafe.Text(err.Error()))
		}
		fmt.Printf(
			"Authentication enabled for %q in %s\n",
			strings.TrimSpace(setAuthUsername),
			logsafe.Text(configPath),
		)
		return
	}

	// Buffer pools are owned by their subsystems: the DFS cache (vfs.NewCache)
	// and the usenet reader each create a buffer.Pool with their own configured
	// RAM budget and disk limit.

	// Start pprof server if enabled
	if pprofAddr != "" && enablePprof {
		if err := validatePprofListenAddress(pprofAddr); err != nil {
			log.Fatalf("refusing unsafe pprof listener: %s", logsafe.Text(err.Error()))
		}
		go func() {
			log.Printf("Starting pprof server on %s", logsafe.Text(pprofAddr))
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				log.Printf("pprof server error: %s", logsafe.Text(err.Error()))
			}
		}()
	}

	// Create a context canceled on SIGINT/SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := tessarr.Start(ctx); err != nil {
		log.Fatalf("Tessarr stopped with an error: %s", logsafe.Text(err.Error()))
	}
}

func validatePprofListenAddress(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("parse %q: %w", address, err)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("pprof host must be a loopback IP literal: %q", host)
	}
	if !ip.IsLoopback() {
		return fmt.Errorf("pprof host must be loopback, got %q", host)
	}
	return nil
}

func configureAuthFromTerminal(_ *config.Config, username string) error {
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return fmt.Errorf(
			"standard input is not a terminal; run --set-auth interactively",
		)
	}

	fmt.Fprint(os.Stderr, "Password: ")
	password, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return fmt.Errorf("read password: %w", err)
	}

	fmt.Fprint(os.Stderr, "Confirm password: ")
	confirmation, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return fmt.Errorf("read password confirmation: %w", err)
	}
	defer clear(password)
	defer clear(confirmation)
	if string(password) != string(confirmation) {
		return fmt.Errorf("passwords do not match")
	}

	if _, err := config.Update(func(draft *config.Config) error {
		return draft.ApplyAuthCredentials(username, string(password))
	}); err != nil {
		return fmt.Errorf("save authentication setting: %w", err)
	}
	return nil
}
