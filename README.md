# infbus

An OpenAI-compatible inference gateway over NATS.
Infbus provides a scalable, distributed inference service that abstracts NATS messaging
into a familiar OpenAI API surface, enabling flexible deployment patterns for AI model serving.

**Status:** pre-alpha

## Quickstart

```sh
docker compose -f deploy/docker-compose.yaml up -d --build
curl http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer ib_dev_change_me" \
  -H "Content-Type: application/json" \
  -d '{"model":"fast","stream":true,"messages":[{"role":"user","content":"hello"}]}'
```

The example worker config points at an OpenAI-compatible engine on
`localhost:11434` (e.g. Ollama running `llama3.2`). Point `deploy/worker.example.yaml`
at any vLLM / llama.cpp / Ollama endpoint you have.

See [docs/design.md](docs/design.md) for architecture and design details.
