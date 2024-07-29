import os
import time
from threading import Lock
import execution

class PipelineConfig:
    threads_count = max(os.environ.get('COMFY_PIPLELINE_THREADS', 6), 1)
    model_count = os.environ.get('COMFY_PIPLELINE_MODEL_COUNT', 1) # parallel run model at same time
    low_mem = True # if vram not capacitate for more model don't alloc


from server import PromptServer
server_instance = PromptServer.instance
origin_promptQueue = execution.PromptQueue(server_instance)

import comfy.model_management

origin_cleanup_models = comfy.model_management.cleanup_models
def mock_cleanup_models(keep_clone_weights_loaded=False):
    pass
comfy.model_management.cleanup_models = mock_cleanup_models


origin_soft_empty_cache = comfy.model_management.soft_empty_cache
def mock_soft_empty_cache(force=False):
    pass
comfy.model_management.soft_empty_cache = mock_soft_empty_cache


origin_put = execution.PromptQueue.put

origin_get = execution.PromptQueue.get
def mock_get(self, timeout):
    while True:
        time.sleep(1)
execution.PromptQueue.get = mock_get

# don't need mock comfy.model_management.unload_all_models


# request from UI or /prompt not /parallel/prompt
from_origin = "origin"

lock = Lock()
number = 0
def inc_number():
    global number
    with lock:
        number += 1
        return number
    
