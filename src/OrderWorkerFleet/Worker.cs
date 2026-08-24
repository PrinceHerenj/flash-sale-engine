using Confluent.Kafka;
using Npgsql;
using System.Text.Json;

namespace OrderWorkerFleet;

public class OrderWorker : BackgroundService
{
    private readonly ILogger<OrderWorker> _logger;
    private readonly ConsumerConfig _consumerConfig;
    private readonly IProducer<string, string> _dlqProducer;
    private readonly string _connectionString;

    public OrderWorker(ILogger<OrderWorker> logger, IConfiguration configuration)
    {
        _logger = logger;
        _connectionString = configuration.GetConnectionString("Orders")
            ?? "Host=localhost;Port=5432;Database=orders_db;Username=engine_user;Password=engine_password;";

        var kafkaBootstrap = configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
        _consumerConfig = new ConsumerConfig
        {
            BootstrapServers = kafkaBootstrap,
            GroupId = "order-persistence-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        _dlqProducer = new ProducerBuilder<string, string>(
            new ProducerConfig { BootstrapServers = kafkaBootstrap }).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var consumer = new ConsumerBuilder<string, string>(_consumerConfig).Build();
        consumer.Subscribe("orders.created");

        var batch = new List<(ConsumeResult<string, string> Msg, OrderMessage Order)>();
        var lastFlush = DateTime.UtcNow;

        while (!stoppingToken.IsCancellationRequested)
        {
            var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(50));
            if (consumeResult != null)
            {
                var order = JsonSerializer.Deserialize<OrderMessage>(consumeResult.Message.Value);
                if (order != null) batch.Add((consumeResult, order));
            }

            if (batch.Count >= 100 || (DateTime.UtcNow - lastFlush > TimeSpan.FromMilliseconds(100) && batch.Count > 0))
            {
                await PersistBatchAsync(batch);
                consumer.Commit(batch.Select(b => b.Msg.TopicPartitionOffset));
                batch.Clear();
                lastFlush = DateTime.UtcNow;
            }
        }
    }

    private async Task PersistBatchAsync(List<(ConsumeResult<string, string> Msg, OrderMessage Order)> batch)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var tx = await conn.BeginTransactionAsync();

        try
        {
            await using var cmd = new NpgsqlCommand(
                @"INSERT INTO orders (order_id, user_id, product_id, quantity, total_price, status, idempotency_key)
                  VALUES (@order_id, @user_id, @product_id, @quantity, @total_price, 'CONFIRMED', @idempotency_key)
                  ON CONFLICT (idempotency_key) DO NOTHING;", conn, tx);

            foreach (var (_, item) in batch)
            {
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("order_id", Guid.Parse(item.OrderId));
                cmd.Parameters.AddWithValue("user_id", item.UserId);
                cmd.Parameters.AddWithValue("product_id", item.ProductId);
                cmd.Parameters.AddWithValue("quantity", item.Quantity);
                cmd.Parameters.AddWithValue("total_price", item.TotalPrice);
                cmd.Parameters.AddWithValue("idempotency_key", item.IdempotencyKey);
                await cmd.ExecuteNonQueryAsync();
            }

            await tx.CommitAsync();
            _logger.LogInformation("Batch persisted: {Count} orders", batch.Count);
        }
        catch (Exception ex)
        {
            await tx.RollbackAsync();
            _logger.LogError(ex, "Failed to persist batch. Forwarding {Count} orders to DLQ...", batch.Count);
            await ForwardBatchToDlqAsync(batch, ex.Message);
        }
    }

    private async Task ForwardBatchToDlqAsync(
        List<(ConsumeResult<string, string> Msg, OrderMessage Order)> batch, string failureReason)
    {
        foreach (var (msg, order) in batch)
        {
            var dlqPayload = new
            {
                OriginalMessage = order,
                FailureReason = failureReason,
                FailedAt = DateTime.UtcNow,
                SourcePartition = msg.Partition.Value,
                SourceOffset = msg.Offset.Value
            };

            try
            {
                await _dlqProducer.ProduceAsync("orders.dlq", new Message<string, string>
                {
                    Key = order.ProductId,
                    Value = JsonSerializer.Serialize(dlqPayload)
                });
            }
            catch (Exception dlqEx)
            {
                _logger.LogCritical(dlqEx, "Failed to forward order {OrderId} to DLQ. Manual intervention required.", order.OrderId);
            }
        }
    }

    public override void Dispose()
    {
        _dlqProducer.Flush(TimeSpan.FromSeconds(5));
        _dlqProducer.Dispose();
        base.Dispose();
    }
}

public record OrderMessage(string OrderId, string UserId, string ProductId, int Quantity, decimal TotalPrice, string IdempotencyKey);
