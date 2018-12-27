local _M = {}
local cjson = require "cjson"
local domain_cache = ngx.shared.domain_cache
local cache = ngx.shared.cache

function GetIPType(ip)
  local R = {ERROR = 0, IPV4 = 1, IPV6 = 2, STRING = 3}
  if type(ip) ~= "string" then return R.ERROR end

  -- check for format 1.11.111.111 for ipv4
  local chunks = {ip:match("^(%d+)%.(%d+)%.(%d+)%.(%d+)$")}
  if #chunks == 4 then
    for _,v in pairs(chunks) do
      if tonumber(v) > 255 then return R.STRING end
    end
    return R.IPV4
  end

  -- check for ipv6 format, should be 8 'chunks' of numbers/letters
  -- without leading/trailing chars
  -- or fewer than 8 chunks, but with only one `::` group
  local chunks = {ip:match("^"..(("([a-fA-F0-9]*):"):rep(8):gsub(":$","$")))}
  if #chunks == 8
  or #chunks < 8 and ip:match('::') and not ip:gsub("::","",1):match('::') then
    for _,v in pairs(chunks) do
      if #v > 0 and tonumber(v, 16) > 65535 then return R.STRING end
    end
    return R.IPV6
  end

  return R.STRING
end

function split(str, delimiter)
    if str == nil or str == '' or delimiter == nil then
        return nil
    end

    local result = {}
    for match in (str .. delimiter):gmatch("(.-)" .. delimiter) do
        table.insert(result, match)
    end
    return result
end

-- 重写URL
function rewrite(request_uri, reg, original_uri)
    i, j = string.find(request_uri, reg)
    if i ~= nil then
        local real_uri, _ = string.gsub(request_uri, reg, original_uri, 1)
        return real_uri
    end
    return nil
end

-- 负载均衡，选择服务器
function select_server(api_info)
    local servers = api_info["servers"]
    local server_count = table.getn(servers)

    if server_count == 0 then
        return nil
    end

    -- 简单轮询策略
    local request_index_cache_key = ngx.var.host .. "_request_index_" .. api_info["request_uri"]
    local request_index, _ = cache:incr(request_index_cache_key, 1)
    if request_index == nil then
        request_index = cache:incr(request_index_cache_key, 1, 0)
    end

    return servers[request_index % server_count + 1]; --Lua 的 table 索引默认从 1 开始
end

function _M.dispatch()
    local config_str = domain_cache:get(ngx.var.host);
    if config_str == nil then
        config_str = domain_cache:get("localhost");
        if config_str == nil then
            ngx.exit(404)
        end
    end

    local config = cjson.decode(config_str)
    local real_uri
    local api_info
    local api_uri_array = config["api_uri_array"]
    local api_uri_map = config["api_uri_map"]
    local uri = ngx.var.uri
    if ngx.var.args ~= nil then
        uri = uri .. "?" .. ngx.var.args
    end

    -- 匹配请求映射获得配置（api_info）
    for _, uri_regx in pairs(api_uri_array) do
        local api_info_t = api_uri_map[uri_regx];
        real_uri = rewrite(uri, api_info_t["request_uri"], api_info_t["original_uri"]);
        if (real_uri ~= nil) then
            api_info = api_info_t
            break;
        end
    end

    if api_info == nil then
        ngx.exit(404)
    end

    -- TODO：IP黑名单...
    -- 限流
    local al = require "access_limit"
    al.checkAccessLimit(api_info["request_uri"], api_info["uri_limit_seconds"], api_info["uri_limit_times"], api_info["ip_uri_limit_seconds"], api_info["ip_uri_limit_times"])

    -- 负载均衡，选择服务器开始
    local server = select_server(api_info);
    if server == nil then
        ngx.exit(503)
    end

    if api_info["host"] == "localhost" then
        api_info["host"] = ngx.var.host
    end

    if server["protocol"] ~= nil and server["protocol"] ~= "" then
        ngx.var.upstream = server["protocol"] .. "servers"
    end
    ngx.var.backend_host = server["ip"]
    ngx.var.backend_port = server["port"]
    ngx.var.newhost = api_info["host"]
    ngx.req.set_header("Host", api_info["host"])

    local uri_t = split(real_uri, "?")
    ngx.req.set_uri(uri_t[1])
    if table.getn(uri_t) == 2 then
        local uri_args = uri_t[2]
        ngx.req.set_uri_args(uri_args)
    else
        ngx.req.set_uri_args({})
    end
end

return _M
