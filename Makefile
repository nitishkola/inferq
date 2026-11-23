.PHONY: up down logs test test-lua test-worker test-api bench bench-compare chaos

up:            ## build and start everything (3 mock workers)
	docker compose up -d --build

down:
	docker compose down -v

logs:
	docker compose logs -f api worker

test: test-lua test-worker test-api

test-lua:      ## queue semantics against a real Redis (needs redis-server installed)
	tests/lua/run_tests.sh

test-worker:
	cd worker && python3 -m pytest -q

test-api:
	cd api && mvn -B -q verify

bench:
	python3 bench/bench.py --rate 50 --duration 30

# Same load with batching off, then on. Compare the throughput lines.
bench-compare:
	MAX_BATCH=1 docker compose up -d worker && sleep 3
	python3 bench/bench.py --rate 40 --duration 30 --label batch1
	MAX_BATCH=8 docker compose up -d worker && sleep 3
	python3 bench/bench.py --rate 40 --duration 30 --label batch8

chaos:
	python3 chaos/chaos_test.py --jobs 300 --kill 2
