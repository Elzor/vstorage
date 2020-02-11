local log = require('log')
local config = require('config.main')
local context = require('api.v1.context')

local STATUS_SUCCESS = 200
local ERROR_ACCESS_DENY = 403
local BAD_REQUEST = 400
local ERROR_HANDLER_NOT_FOUND = -2

return {
    --
    ---------------------------------------------------------------------------
    --
    server_register = function(self)
        local r = context.request(self)
        if r.zone ~= nil then
            log.info("here")
            return r.reply({
                status = STATUS_SUCCESS,
                result = {},
            })
        end
        return r.reply({
            status = BAD_REQUEST,
            result = {},
        })
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