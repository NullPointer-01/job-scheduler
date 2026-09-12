package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"distributed-job-scheduler/internal/config"
	"distributed-job-scheduler/internal/handler"
	"distributed-job-scheduler/internal/metrics"
	"distributed-job-scheduler/internal/scheduler"
	"distributed-job-scheduler/internal/store"
	"distributed-job-scheduler/internal/worker"

	migrate "github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	// Root context
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Load configuration
	conf, err := config.Load()
	if err != nil {
		slog.Error("Failed to load configuration", "err", err)
		os.Exit(1)
	}

	// Database connection
	pool, err := pgxpool.New(context.Background(), conf.DatabaseURL)
	if err != nil {
		slog.Error("Failed to connect to database", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Migrations
	migrateURL := strings.Replace(conf.DatabaseURL, "postgresql://", "pgx5://", 1)
	migrateURL = strings.Replace(migrateURL, "postgres://", "pgx5://", 1)
	m, err := migrate.New("file://migrations", migrateURL)
	if err != nil {
		slog.Error("Failed to create new migrator", "err", err)
		os.Exit(1)
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		slog.Error("Failed to run migrations", "err", err)
		os.Exit(1)
	}

	db := sqlx.NewDb(stdlib.OpenDBFromPool(pool), "pgx")
	st := store.NewPostgresStore(db)

	// Recover crashed jobs
	recovered, err := st.RecoverCrashedJobs(ctx)
	if err != nil {
		slog.Error("Error whiel recovering crashed jobs: , ", "err", err)
	}

	if recovered > 0 {
		metrics.JobsRecovered.Add(float64(recovered))
		slog.Info("Recovered jobs", "count", recovered)
	}

	// Scheduler and Worker pool
	schedulerId := conf.SchedulerId
	if schedulerId == "" {
		schedulerId = uuid.NewString()
	}

	queue := make(chan store.Job, conf.WkrPoolSize*2)
	sch := scheduler.New(schedulerId, st, queue, conf.SchDispatchCount, conf.SchTickInterval, conf.SchLeaseDuration, conf.SchLeaseRecoveryTick)

	poolId := schedulerId // Same as scheduler Id
	workerPool := worker.New(poolId, st, queue, conf.WkrPoolSize, conf.SchLeaseDuration)

	go sch.Run(ctx)
	go workerPool.Run(ctx)

	// Http Server
	slog.Info("Starting server on ", "addr", conf.ServerAddr, "port", conf.ServerPort)
	addr := conf.ServerAddr + ":" + conf.ServerPort

	apiHandler := handler.NewHandler(st)
	apiMux := http.NewServeMux()
	apiHandler.RegisterRoutes(apiMux)

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

	// Metrics Server
	slog.Info("Starting Metrics server on ", "addr", conf.MetricsAddr, "port", conf.MetricsPort)
	metricsAddr := conf.MetricsAddr + ":" + conf.MetricsPort
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())

	metricsServer := &http.Server{
		Addr:    metricsAddr,
		Handler: metricsMux,
	}

	go func() {
		err := metricsServer.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			slog.Error("Failed to start metrics server", "err", err)
		}
	}()

	<-ctx.Done()
	slog.Info("Shutting down application")

	shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	apiServer.Shutdown(shutdownCtx)
	metricsServer.Shutdown(shutdownCtx)
}
