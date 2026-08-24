-- KEYS[1]: stock:{product_id}
-- ARGV[1]: requested_quantity
local stock_key = KEYS[1]
local quantity = tonumber(ARGV[1])

local current_stock = tonumber(redis.call('GET', stock_key) or '0')

if current_stock >= quantity then
    redis.call('DECRBY', stock_key, quantity)
    local remaining = current_stock - quantity
    return {1, remaining}
else
    return {0, current_stock}
end
