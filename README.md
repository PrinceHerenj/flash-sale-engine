# Flash-Sale Order Engine

A high-throughput, low-latency flash-sale order engine on .NET 8. An order flows through Envoy → IngressGateway → InventoryService → OrderWorkerFleet, connected by gRPC, Kafka, Redis, and Postgres. See [`CLAUDE.md`](CLAUDE.md) for the architecture.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) with the Compose plugin
- [.NET 8 SDK](https://dotnet.microsoft.com/download) (only for host-run development)
- [k6](https://k6.io/docs/get-started/installation/) (only for the load test)
- `lua5.3` (only for linting the Lua scripts)

## Run the full stack (Docker)

Builds and starts all seven containers: `envoy`, `ingress-gateway`, `inventory-service`, `order-worker-fleet`, `postgres`, `redis`, and `kafka`.

```bash
docker compose -f deployments/docker-compose.yml up -d --build
```

Check that the services came up healthy:

```bash
docker compose -f deployments/docker-compose.yml ps
docker compose -f deployments/docker-compose.yml logs -f ingress-gateway
```

## Seed test stock

Stock lives in Redis under `stock:{product_id}`:

```bash
docker exec flashsale-redis redis-cli SET stock:iphone-16-pro 100
```

## Load test with k6

Requests enter through Envoy on host port `8080`, which routes `/api/*` to the ingress gateway (reachable only in-network on `:5000`). The burst test fires 5,000 VUs and asserts a `p(99)` latency under 25ms:

```bash
k6 run tests/k6/flash-sale-burst.js
```

Verify orders landed in Postgres:

```bash
docker exec flashsale-postgres psql -U engine_user -d orders_db -c "SELECT count(*) FROM orders;"
```

## Teardown

```bash
docker compose -f deployments/docker-compose.yml down -v   # -v also drops the Postgres volume
```

## Develop on the host

To run the .NET services directly against local infra (defaults in `appsettings.json` point at `localhost`), bring up just the infra and `dotnet run` each service:

```bash
docker compose -f deployments/docker-compose.yml up -d postgres redis kafka
dotnet run --project src/IngressGateway
dotnet run --project src/InventoryService
dotnet run --project src/OrderWorkerFleet
```

> **Note:** Kafka's `ADVERTISED_LISTENERS` is `kafka:9092` (in-network), so host-run services cannot reach Kafka unless you re-advertise it as `localhost`. Prefer the full Dockerized stack for end-to-end runs.

## Build & lint

```bash
dotnet build flash-sale-engine.sln                       # build all services + generated gRPC stubs
lua5.3 -e "loadfile('src/InventoryService/Scripts/reserve_stock.lua')"
lua5.3 -e "loadfile('src/InventoryService/Scripts/release_stock.lua')"
```
