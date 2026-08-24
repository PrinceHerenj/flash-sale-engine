# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

A flash-sale order engine built on .NET 8, designed for high-throughput, low-latency burst traffic. A single order path runs through four services connected by gRPC, Kafka, Redis, and Postgres.

## Architecture

Requests flow through the services in order:

1. **Envoy** (`deployments/envoy/envoy.yaml`) — edge proxy with a local rate limiter (10k req/s token bucket). Routes `/api/*` to the ingress gateway.
2. **IngressGateway** (`src/IngressGateway`) — ASP.NET Core minimal API. The single endpoint `POST /api/v1/orders` does, in order: idempotency lookup in Redis → gRPC `ReserveStock` call to InventoryService → produce an `orders.created` event to Kafka → write the idempotency key to Redis → return `202 Accepted`.
3. **InventoryService** (`src/InventoryService`) — gRPC server exposing `ReserveStock`/`ReleaseStock`. Stock lives in Redis under `stock:{product_id}`. Concurrency safety comes from **Lua scripts** (`Scripts/*.lua`) run atomically via `ScriptEvaluateAsync`; the `DECRBY`/`INCRBY` are never done from C#.
4. **OrderWorkerFleet** (`src/OrderWorkerFleet`) — `BackgroundService` that consumes `orders.created`, batches rows (100 messages or 100ms, whichever first), and bulk-inserts into Postgres in a transaction with manual offset commits. On failure it rolls back and forwards the batch to `orders.dlq`.

Shared contract lives in **Common.Protos** (`src/Common/Protos`) — a project that wraps `proto/inventory/v1/inventory.proto` with `GrpcServices="Both"`, generating both client and server stubs under namespace `FlashSale.Common.Protos.Inventory.V1`. Any change to the `.proto` regenerates stubs on build.

Supporting infra (`deployments/docker-compose.yml`): Postgres 16 (orders table with a `UNIQUE` `idempotency_key` constraint and `ON CONFLICT DO NOTHING` dedup), Redis 7.2 (appendonly), single-node Kafka (KRaft), and Envoy. All four .NET services are dockerized in the same compose file.

**Configuration**: each .NET service reads its infra addresses from `appsettings.json` / environment variables (`Redis__ConnectionString`, `InventoryService__GrpcAddress`, `Kafka__BootstrapServers`, `ConnectionStrings__Orders`), defaulting to `localhost` for host-run development and overridden to compose service names (`redis`, `inventory-service`, `kafka`, `postgres`) by `docker-compose.yml`. `OrderWorkerFleet` uses `GetConnectionString("Orders")` for Postgres.

## Commands

```bash
# Build (all services + generated gRPC stubs)
dotnet build flash-sale-engine.sln

# Build & run the full stack (infra + 4 .NET services) via Docker
docker compose -f deployments/docker-compose.yml up -d --build
docker compose -f deployments/docker-compose.yml down -v   # teardown

# Seed test stock
docker exec flashsale-redis redis-cli SET stock:iphone-16-pro 100

# Load test (5,000 VUs, 99th pct < 25ms threshold; hits Envoy on :8080)
k6 run tests/k6/flash-sale-burst.js

# Lint the Lua scripts
lua5.3 -e "loadfile('src/InventoryService/Scripts/reserve_stock.lua')"
lua5.3 -e "loadfile('src/InventoryService/Scripts/release_stock.lua')"
```

There are no .NET unit tests yet — `tests/` contains only the k6 load test, so `dotnet test` no-ops.

## Known gotchas

- The `.proto` file is referenced from `Common.Protos.csproj` via a relative `../../../proto` path — the **Dockerfiles and repo-root build context** depend on the `proto/` directory staying at the repo root.
- Kafka's `ADVERTISED_LISTENERS` is `kafka:9092` (in-network), so the Dockerized services reach it correctly, but the .NET services run on the host via `dotnet run` will **not** be able to reach Kafka without re-advertising it as `localhost`.
- The `InventoryService` project's root namespace collides with the generated `InventoryService` gRPC class — reference the server base type fully-qualified (`FlashSale.Common.Protos.Inventory.V1.InventoryService.InventoryServiceBase`), as `InventoryGrpcServiceImpl.cs` does.
