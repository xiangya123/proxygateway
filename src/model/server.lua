local mysql = require "model.mysql"
local server_model = {}

function server_model.add(service_id, ip, port, weight, description, protocol)
    description = ndk.set_var.set_quote_sql_str(description)
    local db = mysql.getDb()
    local res, err, _, _ = db:query("INSERT INTO agw_server(service_id,ip,port,weight,description,protocol)values(\'" .. service_id .. "\',\'" .. ip .. "\',\'" .. port .. "\',\'" .. weight .. "\'," .. description .. ",\'" .. protocol .. "\')", 10)
    db:set_keepalive(10000, 100)
    return res, err
end

function server_model.delete(server_id)
    local db = mysql.getDb()
    local res, err, _, _ = db:query("DELETE FROM agw_server WHERE id=" .. server_id, 10)
    db:set_keepalive(10000, 100)
    return res, err
end

function server_model.deleteByServiceId(sid)
    local db = mysql.getDb()
    local res, err, _, _ = db:query("DELETE FROM agw_server WHERE service_id=" .. sid, 10)
    db:set_keepalive(10000, 100)
    return res, err
end

function server_model.update(server_id, ip, port, weight, description, protocol)
    description = ndk.set_var.set_quote_sql_str(description)
    local db = mysql.getDb()
    local res, err, _, _ = db:query("UPDATE agw_server SET ip=\'" .. ip .. "\',port=" .. port .. ",protocol=\'" .. protocol .. "\',weight=\'" .. weight .. "\',description=" .. description .. " WHERE id=" .. server_id, 10)
    db:set_keepalive(10000, 100)
    return res, err
end

function server_model.getServers(state)
    local db = mysql.getDb()
    local servers, _, _, _ = db:query("SELECT * FROM agw_server WHERE status=" .. state, 10)
    db:set_keepalive(10000, 100)
    if not servers then
        return
    end
    return servers
end

function server_model.getServiceServers(service_id)
    local db = mysql.getDb()
    local servers, err, errno, sqlstate = db:query("SELECT * FROM agw_server WHERE service_id=" .. service_id, 10)
    return servers, err
end

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

function getIpFromDomain(domain)
        local resolver = require "resty.dns.resolver"
        local r, err = resolver:new{
                nameservers = {"dns9.hichina.com"},
                retrans = 5,  -- 5 retransmissions on receive timeout
                timeout = 2000,  -- 2 sec
            }
        local answers, err, tries = r:query(domain, nil, {})
        return answers[1].address 
end

function server_model.getServiceServersWithState(service_id, state)
    local db = mysql.getDb()
    local servers, err, _, _ = db:query("SELECT * FROM agw_server WHERE service_id=" .. service_id .. " AND status=" .. state, 10)
    for k, v in ipairs(servers) do
	local ip_type = GetIPType(servers[k]['ip'])
	if ip_type == 3 then
	    servers[k]['ip'] = getIpFromDomain(servers[k]['ip'])
	end
    end
    db:set_keepalive(10000, 100)
    return servers, err
end

function server_model.getServer(id)
    local db = mysql.getDb()
    local servers, err, _, _ = db:query("SELECT * FROM agw_server WHERE id=" .. id, 10)
    db:set_keepalive(10000, 100)
    server = nil
    if table.getn(servers) > 0 then
        server = servers[1]
    else
        err = "error server id"
    end
    return server, err
end

function server_model.getServersByMid(mid)
    local db = mysql.getDb()
    local servers, _, _, _ = db:query("SELECT * FROM agw_server WHERE mid=" .. mid, 10)
    db:set_keepalive(10000, 100)
    if not servers then
        return
    end
    return servers
end

function server_model.getAllServers()
    local db = mysql.getDb()
    local servers, _, _, _ = db:query("SELECT * FROM agw_server", 10)
    db:set_keepalive(10000, 100)
    if not servers then
        return
    end
    return servers
end

function server_model.updateState(sid, state)
    local db = mysql.getDb()
    local res, err, _, _ = db:query("UPDATE agw_server SET status=" .. state .. " WHERE id=" .. sid, 10)
    db:set_keepalive(10000, 100)
    if not res then
        ngx.log(ngx.ERR, "update server state err:", err);
        return false
    end
    return true
end

return server_model
