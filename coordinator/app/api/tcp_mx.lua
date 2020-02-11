local api_v1 = require('api.v1.controller')
return {
    handle = function(conn, req)
        local controller = api_v1
        if req["n"] ~= nil then
            controller = "api_v" .. tostring(req["n"])
        end
        local handler = nil
        if controller[req.m] ~= nil then
            handler = controller[req.m]
        else
            handler = controller.default
        end
        return handler({
            endpoint = {
                format = "tcp"
            },
            conn = conn,
            req = req.v,
            method = req.m,
        })
    end,
}