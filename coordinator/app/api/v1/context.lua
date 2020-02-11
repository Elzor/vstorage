local m = {}

local log = require('log')
local msgpack = require('msgpack')
local json = require('json')

function m.request(r)
    local format = m.api_format(r)
    local res = {}
    if format == "tcp" then
        res = r.req
        res.method = r.method
        res.stash = function(name)
            return res[name]
        end
    else
        if r.method == "POST" or r.method == "PUT" then
            if 'json' == format then
                res = r:json()
            elseif 'msgpack' == format then
                local ok, d = pcall(msgpack.decode, r:read_cached())
                if ok then
                    res = d
                end
            end
        end
    end
    res.reply = function(params)
        return m.render(r, format, params)
    end
    return res
end

function m.render(r, format, params)
    if 'tcp' == format then
        m.tcp_reply(r.conn, params)
    elseif 'json' == format then
        local ok, b = pcall(json.encode, params.result)
        if not ok then
            b = ''
        end
        return {
            status = params.status,
            headers = {
                ['content-type'] = 'application/json',
                ['server'] = 'vstor-c/1',
            },
            body = b
        }
    elseif 'msgpack' == format then
        local ok, b = pcall(msgpack.encode, setmetatable(params.result, { __serialize = "map" }))
        if not ok then
            b = ''
        end
        return {
            status = params.status,
            headers = { ['content-type'] = 'application/x-msgpack' },
            body = b
        }
    end
end

function m.api_format(r)
    local format = r.endpoint.format or 'json'
    if format == "tcp" then
        return format
    else
        local accept = string.lower(r.headers.accept or format)
        if accept == 'application/x-msgpack' or accept == 'msgpack' then
            accept = 'msgpack'
        else
            accept = 'json'
        end
        return accept
    end
end

function m.tcp_reply(s, r)
    local response = msgpack.encode(setmetatable(r, { __serialize = "map" }))
    s:send(tostring(string.len(response)) .. ":" .. response)
end

return m