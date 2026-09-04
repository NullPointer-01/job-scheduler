build:
	go build -o bin/scheduler ./cmd/scheduler

run:
	go run ./cmd/scheduler

migrate-up:
	migrate -path migrations/ -database "$(DATABASE_URL)" up

migrate-down:
	migrate -path migrations/ -database "$(DATABASE_URL)" down