local log = require('log')

local server = {
    idxs = {
        primary = 'prim',
        zone = 'zone',
        rack = 'rack',
        type = 'type',
    },
    fields = {
        id = 1,
        type = 2,
        zone = 3,
        rack = 4,
        created = 5,
        updated = 6,
        payload = 7,
    },
}

function server.init(config)
    server.config = config
    server.space = config.spaces.servers

    local space = box.schema.space.create(config.spaces.servers, {
        engine = 'memtx',
        if_not_exists = true,
    })
    log.info('space configured: %s', config.spaces.servers)

    space:create_index(server.idxs.primary, {
        type = 'HASH',
        parts = {
            server.fields.id, 'string',
        },
        unique = true,
        if_not_exists = true,
    })
    log.info('servers primary index created')

    return space
end

return server
