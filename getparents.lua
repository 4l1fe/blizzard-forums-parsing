local result = {};

local node = redis.call('hgetall', KEYS[1]);
table.insert(result, node);

while true do
    node = redis.call('hgetall', node[2]);

    if node[2] == nil then
        break
    end
    table.insert(result, node);
end;

return result