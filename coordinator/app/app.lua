package.path = "/opt/vstorage/block_coordinator/?.lua;" .. package.path

local log = require('log')
local config = require('config.main')
local api_v1 = require('api.v1.controller')
local api_tcp_mx = require('api.tcp_mx')
local strict = require('strict')

strict.on()

--
-- Box -----------------------------------------------------------------------------
--
box.cfg {
    listen = config.listen_port,
    custom_proc_title = "vstorage-coordinator",
    feedback_enabled = false,
}
box.schema.user.grant('guest', 'read,write,execute', 'universe', '', { if_not_exists = true })
log.info('box configured: %s', box.info.version)

--
-- Models --------------------------------------------------------------------------
--
models = {
    server = require('models.server'),
}
--
-- Spaces --------------------------------------------------------------------------
--
spaces = {
    servers = models.server.init(config),
}

--
------------------------------------------------------------------------------------
--

require('http.server').new(config.http_host, config.http_port, {
    charset = 'utf-8',
    display_errors = false,
    log_requests = false,
})
    :route({ path = '/', method = 'GET' }, api_v1.index)
    :route({ path = '/ping', method = 'GET' }, api_v1.ping)
    :route({ path = '/v1', method = 'GET' }, api_v1.index)
    :route({ path = '/v1/server/register', method = 'POST' }, api_v1.server_register)
--    :route({ path = '/v1/server/leave', api_v1.server_leave })
--    :route({ path = '/v1/block/prepare', api_v1.block_prepare })
--    :route({ path = '/v1/block/commit', api_v1.block_commit })
--    :route({ path = '/v1/block/whereis', api_v1.block_whereis })
:start()