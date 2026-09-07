# Infbus Compose Infrastructure

Run `docker compose -f deploy/docker-compose.yaml up -d --build` to boot the complete stack. The gateway serves the OpenAI-compatible API on port 8080 with bearer token auth (configure via `gateway.example.yaml`, example key `ib_dev_change_me`). The worker processes inference requests. Postgres and ClickHouse are provisioned for M3/M4 and idle until then.
