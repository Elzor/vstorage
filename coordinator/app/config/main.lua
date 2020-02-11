return {
    listen_port = 3301,
    http_host = '0.0.0.0',
    http_port = 8800,

    api = {
        supported_vsn = {'v1', 'v1m'}
    },

    spaces = {
        servers = 'servers',
        slots = 'slots',
        blocks = 'blocks',
    },

    memtx_memory = 268435456,
}