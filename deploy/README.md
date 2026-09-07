# Infbus Compose Infrastructure

Run `docker compose -f deploy/docker-compose.yaml up -d` to boot the infra (nats JetStream, postgres, clickhouse). App services (gateway/worker) join in a later milestone. Postgres/ClickHouse are provisioned for M3/M4 and idle until then.
