using Confluent.Kafka;
using StackExchange.Redis;
using System.Text.Json;
using FlashSale.Common.Protos.Inventory.V1;

var bulder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<IConnectionMultiplexer>(ConnectionMultiplexer.Connect("localhost:6379"));
builder.Services.AddGrpcClient<FlashSale.Common.Protos.Inventory.V1.InventoryService.InventoryServiceClient>(o =>
{
    o.address = new Uri("http://localhost:50051");
});

var producerConfig = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    Acks = Acks.All,
    EnableIdempotence = true,
    CompressionType = CompressionType.Snappy
};
builder.Services.AddSingleton(new ProducerBuilder<string, string>(producerConfig).Build());

var app = builder.Build();

app.MapPost("/api/v1/orders", async (
    OrderRequest request,
    HttpContext context,
    IConnectionMultiplexer redis,
    FlashSale.Common.Protos.Inventory.V1.InventoryService.InventoryServiceClient inventoryClient,
    IProducer<string, string> kafkaProducer) => {
        var idempotencyKey = context.Request.Headers["Idempotency-Key"].ToString();
        if (string.IsNullOrEmpty(idempotencyKey))
        {
            return Results.BadRequest(new { error = "Missing Idempotency-Key header" });
        }

        var redisDb = redis.GetDatabase();

        var cachaedOrderId = await redisDb.StringGetAsync($"idempotency:{idempotencyKey");

        if (cachedOrderId.HasValue)
        {
            return Results.Ok(new {message = "Duplicate request processed", order_id = cachaedOrderId.ToString(), status = "DUPLICATE"});
        }

        var grpcResponse = await inventoryClient.ReserveStockAsync(new ReserverStockRequest {
            ProductId = request.ProductId,
            Quantity = request.Quantity,
            UserId = request.UserId
        });

        if (!grpcResponse.Success) {
            return Results.Json(new { error = grpcResponse.Message}, statusCode: StatusCodes.Status409Conflict);
        }

        var orderId = Guid.NewGuid().ToString();
        var orderEvent = new {
            OrderId = orderId,
            request.UserId,
            request.ProductId,
            request.Quantity,
            request.UnitPrice,
            TotalPrice = request.Quantity * request.UnitPrice,
            IdempotencyKey = idempotencyKey,
            Timestamp = DateTime.UtcNow
        };

        await kakpaProducer.ProduceAsync("orders.created", new Message<string, string>
            {
                Key = request.ProductId,
                Value = JsonSerializer.Serialize(orderEvent)
            });

        await redisDb.StringSetAsync($"idempotency:{idempotencyKey}", orderId, TimeSpan.FromHours(24));

        return Results.Accepted($"/api/v1/orders/{orderId}", new {order_id = orderId, status = "PENDING"});
});

app.Run();

public record OrderRequest(string UserId, string ProductId, int Quantity, decimal UnitPrice);
