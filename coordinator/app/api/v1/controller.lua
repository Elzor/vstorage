local log = require('log')
local crypto = require('crypto')
local config = require('config.main')
local context = require('api.v1.context')

local reply = function(status, result)
    return {
        status = status,
        result = result,
    }
end

local STATUS_SUCCESS = 200
local ERROR_ACCESS_DENY = 403
local BAD_REQUEST = 400
local BAD_REQUEST_REPLY = {
    field_requred = function(field)
        return {
            status = BAD_REQUEST,
            result = {
                msg = 'fields is required',
                field = field,
            }
        }
    end
}
local ERROR_HANDLER_NOT_FOUND = -2

return {
    --
    ---------------------------------------------------------------------------
    --
    server_register = function(self)
        local r = context.request(self)
        if r.server == nil then
            return r.reply(BAD_REQUEST_REPLY.field_requred('server'))
        end
        if r.server.nodename == nil then
            return r.reply(BAD_REQUEST_REPLY.field_requred('server.nodename'))
        end
        if r.server.zone == nil then
            return r.reply(BAD_REQUEST_REPLY.field_requred('server.zone'))
        end
        if r.server.srv_type == nil then
            return r.reply(BAD_REQUEST_REPLY.field_requred('server.srv_type'))
        end

        local id = crypto.digest.sha1(r.server.zone .. r.server.rack .. r.server.nodename .. r.server.srv_type):hex()
        if spaces.servers:get(id) == nil then
            log.info('register ' .. id)
        end
        return reply(STATUS_SUCCESS, {id = id})
    end,

    --
    ---------------------------------------------------------------------------
    --
    index = function(self)
        local r = context.request(self)
        return r.reply({
            status = STATUS_SUCCESS,
            result = {
                api = config.api.supported_vsn,
                welcome = 'The little coordinator engine that could!',
            },
        })
    end,

    --
    ---------------------------------------------------------------------------
    --
    ping = function(self)
        local r = context.request(self)
        return r.reply({
            status = STATUS_SUCCESS,
            result = {
                pong = true
            },
        })
    end,

    --
    ---------------------------------------------------------------------------
    --
    default = function(self)
        local r = context.request(self)
        return r.reply({
            status = ERROR_HANDLER_NOT_FOUND,
            result = {
                m = r.method
            },
        })
    end,
}