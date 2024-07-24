import os
from threading import Lock

class PipelineConfig:
    threads_count = max(os.environ.get('COMFY_PIPLELINE_THREADS', 6), 1)
    model_count = os.environ.get('COMFY_PIPLELINE_MODEL_COUNT', 1) # parallel run model at same time
    low_mem = True # if vram not capacitate for more model don't alloc


import comfy.model_management

origin_cleanup_models = comfy.model_management.cleanup_models
def mock_cleanup_models(keep_clone_weights_loaded=False):
    pass
comfy.model_management.cleanup_models = mock_cleanup_models


origin_soft_empty_cache = comfy.model_management.soft_empty_cache
def mock_soft_empty_cache(force=False):
    pass
comfy.model_management.soft_empty_cache = mock_soft_empty_cache

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
    
