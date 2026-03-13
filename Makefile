genproto:
	@find ./proto -name '*.proto' -exec protoc --proto_path=./proto --go_out=./gen --go_opt=paths=source_relative --go-vtproto_out=./gen --go-vtproto_opt=paths=source_relative,features=marshal+unmarshal+size+grpc {} +

test-up:
	docker compose up -d --wait
	@echo "Redis Cluster is ready (3 masters + 3 replicas)."

test-down:
	docker compose down -v

test: test-up
	go test -v -count=1 -timeout=300s ./tests/integration/

test-failure: test-up
	go test -v -count=1 -timeout=300s ./tests/failure/

test-unit:
	go test -v -count=1 ./internal/...

bench: test-up
	go test -bench=. -benchmem -count=1 -timeout=300s ./bench/

bench-short: test-up
	go test -bench=BenchmarkEnqueue -benchmem -count=1 -timeout=60s ./bench/
