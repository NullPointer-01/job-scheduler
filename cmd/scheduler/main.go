package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-job-scheduler/internal/config"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	conf, err := config.Load()
	if err != nil {
		slog.Error("Failed to load configuration", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Http Server
	slog.Info("Starting server on ", "addr", conf.ServerAddr, "port", conf.ServerPort)
	addr := conf.ServerAddr + ":" + conf.ServerPort

	apiMux := http.NewServeMux()
	apiMux.HandleFunc("/", indexHandler)

	apiServer := &http.Server{
		Addr:    addr,
		Handler: apiMux,
	}

	go func() {
		err := apiServer.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			slog.Error("Failed to start server", "err", err)
		}
	}()

	<-ctx.Done()
	slog.Info("Shutting down application")

	shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	apiServer.Shutdown(shutdownCtx)
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Hello, Scheduler!")
}
