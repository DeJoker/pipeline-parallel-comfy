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
routes = PromptServer.instance.routes

from .parallel_execution import parallel_prompt_queue

number = 0

@routes.post("/parallel/prompt")
async def post_prompt(request):
    print("got prompt")
    resp_code = 200
    json_data =  await request.json()

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
