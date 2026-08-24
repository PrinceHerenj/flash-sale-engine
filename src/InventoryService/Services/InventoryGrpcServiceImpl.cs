using Grpc.Core;
using StackExchange.Redis;
using FlashSale.Common.Protos.Inventory.V1;

namespace InventoryService.Services;

public class InventoryGrpcServiceImpl : FlashSale.Common.Protos.Inventory.V1.InventoryService.InventoryServiceBase
{
    private readonly IDatabase _redisDb;
    private readonly string _reserveScript;
    private readonly string _releaseScript;

    public InventoryGrpcServiceImpl(IConnectionMultiplexer redis)
    {
        _redisDb = redis.GetDatabase();
        _reserveScript = File.ReadAllText("Scripts/reserve_stock.lua");
        _releaseScript = File.ReadAllText("Scripts/release_stock.lua");
    }

    public override async Task<ReserveStockResponse> ReserveStock(ReserveStockRequest request, ServerCallContext context)
    {
        var stockKey = $"stock:{request.ProductId}";
        var result = (RedisValue[]?)await _redisDb.ScriptEvaluateAsync(
            _reserveScript,
            new RedisKey[] { (RedisKey)stockKey },
            new RedisValue[] { request.Quantity }
        );

        if (result != null && (int)result[0] == 1)
        {
            return new ReserveStockResponse
            {
                Success = true,
                Message = "Stock reserved successfully",
                RemainingStock = (int)result[1]
            };
        }

        return new ReserveStockResponse
        {
            Success = false,
            Message = "Insufficient inventory",
            RemainingStock = result != null ? (int)result[1] : 0
        };
    }

    public override async Task<ReleaseStockResponse> ReleaseStock(ReleaseStockRequest request, ServerCallContext context) {
        var stockKey = $"stock:{request.ProductId}";
        var updated = await _redisDb.ScriptEvaluateAsync(
            _releaseScript,
            new RedisKey[] { (RedisKey)stockKey },
            new RedisValue[] { request.Quantity }
        );

        return new ReleaseStockResponse
        {
            Success = true,
            UpdatedStock = (int)updated
        };
    }
}
