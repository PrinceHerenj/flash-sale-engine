using InventoryService.Services;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc();
builder.Services.AddSingleton<IConnectionMultiplexer>(
    ConnectionMultiplexer.Connect(builder.Configuration["Redis:ConnectionString"] ?? "localhost:6379"));

var app = builder.Build();

app.MapGrpcService<InventoryGrpcServiceImpl>();
app.MapGet("/", () => "Inventory Service is running. Use a gRPC client.");

app.Run();
