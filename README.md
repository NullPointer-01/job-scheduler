# Distributed Job Scheduler

A lightweight **distributed job scheduler built with Go and PostgreSQL**. It provides an HTTP API to schedule, monitor, and cancel jobs, with concurrent execution through a configurable worker pool.

## Features

* Schedule jobs for immediate or future execution
* Concurrent job execution with worker pools
* Configurable retries
* Per-job execution timeouts
* PostgreSQL persistence
* Crash recovery for running jobs
* Lease-based job ownership and expiry
* Support for multiple scheduler instances and worker pools
* Prometheus metrics

## Architecture

```text
HTTP API
   │
   ▼
PostgreSQL ◄──── Scheduler 1 ────► Worker Pool 1
   ▲                    │
   │                    ▼
   │               Job Handlers
   │
   └──────────── Scheduler 2 ────► Worker Pool 2
                        │
                        ▼
                   Job Handlers
```

Multiple scheduler instances can run concurrently, each with its own worker pool. PostgreSQL acts as the shared persistent state store.

Schedulers use **leases** to establish ownership of jobs. A lease has an expiry time, allowing ownership to be recovered when a scheduler or worker fails before completing the job.

```text
Job
 │
 ▼
Lease acquired
 │
 ├── Scheduler/worker succeeds
 │       └──► Lease released
 │
 └── Scheduler/worker fails
         │
         ▼
      Lease expires
         │
         ▼
   Job becomes recoverable
         │
         ▼
 Another scheduler can acquire the lease
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
