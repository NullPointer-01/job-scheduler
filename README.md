# Distributed Job Scheduler

A lightweight **distributed job scheduler built with Go and PostgreSQL**. It provides an HTTP API to schedule, monitor, and cancel jobs, with concurrent execution through a configurable worker pool.

## Features

* Schedule jobs for immediate or future execution
* Concurrent job execution with worker pools
* Configurable retries
* Per-job execution timeouts
* PostgreSQL persistence
* Crash recovery for running jobs
* Prometheus metrics
* Extensible job handlers
* Graceful shutdown

## Architecture

```text
HTTP API
   │
   ▼
PostgreSQL ◄── Scheduler
                  │
                  ▼
             Worker Pool
                  │
                  ▼
             Job Handlers
```

## Tech Stack

* Go
* PostgreSQL
* pgx / sqlx
* golang-migrate
* Prometheus

## Quick Start

### Requirements

* Go 1.26+
* PostgreSQL

### Run

```bash
git clone https://github.com/NullPointer-01/job-scheduler.git
cd job-scheduler

go mod download
go run ./cmd/scheduler
```

Set `DATABASE_URL` if you need a custom PostgreSQL connection:

```bash
export DATABASE_URL="postgresql://postgres:password@localhost:5432/mydb"
```

## Job Lifecycle

```text
scheduled → running → success
                 └──→ failed → retry
                 └──→ cancelled
```


## Project Structure

```text
cmd/              # Application entry point
internal/
  handler/        # HTTP API
  scheduler/      # Job scheduling
  worker/         # Worker pool
  store/          # PostgreSQL persistence
  config/         # Configuration
  metrics/        # Prometheus metrics
migrations/       # Database migrations
```

## License

MIT
