import execution
import uuid
import urllib
import json
from PIL import Image, ImageOps
from PIL.PngImagePlugin import PngInfo
from io import BytesIO

import aiohttp
from aiohttp import web


from server import PromptServer
server_inst = PromptServer.instance
routes = PromptServer.instance.routes

from .parallel_execution import parallel_prompt_queue

number = 0

@routes.post("/parallel/prompt")
async def post_prompt(request):
    print("parallel got prompt")
    resp_code = 200
    json_data = await request.json()

    global number
    number += 1

    if "prompt" not in json_data:
        return web.json_response({"error": "no prompt", "node_errors": []}, status=400)
    
    prompt = json_data["prompt"]
    valid = execution.validate_prompt(prompt)
    extra_data = {}
    if "extra_data" in json_data:
        extra_data = json_data["extra_data"]
        if "workflow_name" not in extra_data:
            return web.json_response({"error": "no workflow_name", "node_errors": []}, status=400)

    if "client_id" in json_data:
        extra_data["client_id"] = json_data["client_id"]
    
    if valid[0]:
        prompt_id = str(uuid.uuid4())
        outputs_to_execute = valid[2]
        workflow_name = extra_data["workflow_name"]
        parallel_prompt_queue.put(workflow_name, (number, prompt_id, prompt, extra_data, outputs_to_execute))
        response = {"prompt_id": prompt_id, "number": number, "node_errors": valid[3]}
        return web.json_response(response)
    else:
        print("invalid prompt:", valid[1])
        return web.json_response({"error": valid[1], "node_errors": valid[3]}, status=400)


@routes.get("/parallel/history/{prompt_id}")
async def get_history(request):
    prompt_id = request.match_info.get("prompt_id", None)
    return web.json_response(parallel_prompt_queue.get_history(prompt_id=prompt_id))


@routes.get("/parallel/history")
async def get_history(request):
    max_items = request.rel_url.query.get("max_items", None)
    if max_items is not None:
        max_items = int(max_items)
    return web.json_response(parallel_prompt_queue.get_history(max_items=max_items))


@routes.get('/parallel/ws')
async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    sid = request.rel_url.query.get('clientId', '')
    if sid:
        # Reusing existing session, remove old
        server_inst.sockets.pop(sid, None)
    else:
        sid = uuid.uuid4().hex

    server_inst.sockets[sid] = ws

    try:
        # Send initial state to the new client
        await server_inst.send("status", { "status": server_inst.get_queue_info(), 'sid': sid }, sid)
        # On reconnect if we are the currently executing client send the current node
        # if self.client_id == sid and self.last_node_id is not None:
        #     await self.send("executing", { "node": self.last_node_id }, sid)
            
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.ERROR:
                print('ws connection closed with exception %s' % ws.exception())
    finally:
        server_inst.sockets.pop(sid, None)
    return ws
