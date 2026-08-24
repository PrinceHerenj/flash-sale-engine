-- KEYS[1]: stock:{product_id}
-- ARGV[1]: release_quantity
local stock_key = KEYS[1]
local quantity = tonumber(ARGV[1])

local updated_stock = redis.call('INCRBY', stock_key, quantity)
return updated_stock
