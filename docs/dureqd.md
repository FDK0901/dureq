# dureqd — Standalone Daemon

`dureqd` is a standalone daemon that runs dureq as an independent process,
exposing built-in handlers, HTTP/gRPC monitoring APIs, and health endpoints.

## Quick Start

```bash
# 1. Configure Redis connection
cp configs/config.yaml configs/myconfig.yaml
vi configs/myconfig.yaml

# 2. Run
DUREQ_CONFIG_PATH=configs/myconfig.yaml go run ./cmd/dureqd

# 3. Verify
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
```

## Embedded vs Daemon

| Aspect | Embedded (`dureq` library) | Daemon (`dureqd`) |
|--------|---------------------------|-------------------|
| Handler definition | Go code, `srv.RegisterHandler()` | Static registry + config filtering |
| Deployment | Part of your app binary | Independent binary |
| Health checks | Implement yourself | `/healthz`, `/readyz` built-in |
| Mode separation | `server.WithMode()` option | `mode` config field |
| Custom handlers | Unlimited | Built-in registry only |
| Best for | Custom business logic handlers | Webhook forwarding, echo, testing |

## Configuration

Config is loaded from `configs/config.yaml` by default.
Override with `DUREQ_CONFIG_PATH` environment variable.

See [configs/config.yaml](../configs/config.yaml) for a fully annotated sample.

### Key Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dureqd.nodeId` | string | auto | Unique node identifier |
| `dureqd.apiAddress` | string | `:8080` | HTTP API listen address |
| `dureqd.allowedOrigins` | []string | none | Origins whose web pages may use the HTTP API beside its own, such as `https://durequi.example.com`; `*` allows all |
| `dureqd.allowedHosts` | []string | all | Hosts that the HTTP API answers, such as `dureq.internal` (any port) or `localhost:8080`; others get 403 |
| `dureqd.grpcAddress` | string | `:9090` | gRPC API listen address |
| `dureqd.concurrency` | int | `100` | Max concurrent handlers |
| `dureqd.prefix` | string | `dureq` | Redis key prefix |
| `dureqd.mode` | string | `full` | Subsystem mode (see below) |
| `dureqd.handlers` | []string | all | Handler filter list |
| `dureqd.drainTimeoutMs` | int | `30000` | Shutdown drain timeout (ms) |
| `dureqd.runHistoryRetentionHours` | int | `0` | Remove finished runs from the run history after so many hours; 0 = only by hand |
| `dureqd.maxNameScan` | int | `100000` | How many workflows or batches a list filtered by name (`?name=`) looks at, at most; each is read |

### Modes

| Mode | Subsystems |
|------|-----------|
| `full` | Queue + Scheduler + Workflow + Monitor |
| `queue` | Worker only (processes jobs) |
| `scheduler` | Scheduler + Leader election |
| `workflow` | Workflow orchestrator |
| `monitor` | Monitoring API only |

Combine modes: `"queue,scheduler"` runs both queue processing and scheduling.

## Built-in Handlers

| Task Type | Description |
|-----------|-------------|
| `echo` | Prints payload message to stdout |
| `noop` | Completes immediately (pipeline testing) |
| `fail-always` | Always fails (retry/DLQ testing) |
| `sleep` | Sleeps for configured duration |
| `http-webhook` | Forwards payload to an HTTP endpoint |

### Handler Filtering

To run only specific handlers:

```yaml
dureqd:
  handlers:
    - echo
    - http-webhook
```

Omit the `handlers` field to register all available handlers.

## Health Endpoints

### `/healthz` — Liveness

Returns 200 if Redis is reachable. Use for Kubernetes liveness probes.

```bash
curl -s http://localhost:8080/healthz | jq .
# {"status": "healthy"}
```

### `/readyz` — Readiness

Returns 200 when:
1. Server is fully started
2. Handlers are registered
3. Redis is reachable (verified on each request)

Use for Kubernetes readiness probes and load balancer health checks.

```bash
curl -s http://localhost:8080/readyz | jq .
# {"status": "ready", "handlers": 5}
```

During shutdown, `/readyz` immediately returns 503 to drain traffic
before in-flight jobs complete.

## Graceful Shutdown

On `SIGINT` or `SIGTERM`:

1. `/readyz` returns 503 immediately (stops receiving new traffic)
2. gRPC server stops accepting new connections
3. HTTP server begins draining
4. Worker waits for in-flight jobs up to `drainTimeoutMs` (default: 30s)
5. Process exits

Configure the drain timeout:

```yaml
dureqd:
  drainTimeoutMs: 60000  # 60 seconds
```

## Monitoring API

The full dureq monitoring API is available at `http://localhost:8080/api/...`.
See the dashboard at `http://localhost:8080/` when running with the durequi frontend.

gRPC reflection is enabled for tools like `grpcurl`:

```bash
grpcurl -plaintext localhost:9090 list
```

### Who may use the API

Neither API has authentication, and both can change state: cancel and delete jobs, pause queues, remove history.
Authentication is up to what is in front of them. Do not let them be reached without it.

What the HTTP API does by itself is to keep web pages of other origins out, for a browser reaches whatever its user
reaches. Requests and WebSockets of another origin are answered with 403. Those without an origin, such as of `curl`
or a server, are not affected.

- **The UI is served by another host, port or scheme than the API:** list its origin.

  ```yaml
  dureqd:
    allowedOrigins:
      - "https://durequi.example.com"
  ```

- **A proxy serves both as one origin:** it has to pass the host on that it was asked for, as `Host` or
  `X-Forwarded-Host`. With nginx:

  ```nginx
  location /api/ {
      proxy_pass http://dureqd:8080;
      proxy_http_version 1.1;
      proxy_set_header Host $host;   # $http_host if the port is not the default one
      proxy_set_header Upgrade $http_upgrade;
      proxy_set_header Connection "upgrade";
  }
  ```

`"*"` allows every origin, so that any web page can control the cluster through the browser of who looks at it.

A web page can also get around the origin check by DNS rebinding: its own name, such as `rebind.attacker.example`,
comes to point at the address of the API, and to the browser the page and the API are then of the same origin. So:

- **Authentication in front of the API is required**, not only the origin check. A page that rebinds does not have
  the credentials of the API's own name.
- **List the hosts that the API is reached by**, so that it answers no other name:

  ```yaml
  dureqd:
    allowedHosts:
      - "dureq.internal"     # any port
      - "localhost:8080"     # this port only
  ```

  The host is the `Host` header and, behind a proxy that sets it, `X-Forwarded-Host`; both have to be listed if they
  differ. By default all hosts are answered, as before. In a server of your own it is `API.AllowHosts`.
