import os
from threading import Lock


class PipelineConfig:
    threads_count = max(os.environ.get('COMFY_PIPLELINE_THREADS', 6), 1)
    model_count = os.environ.get('COMFY_PIPLELINE_MODEL_COUNT', 1) # parallel run model at same time
    low_mem = True # if vram not capacitate for more model don't alloc

# request from UI or /prompt not /parallel/prompt
from_origin = "origin"

lock = Lock()
number = 0
def inc_number():
    global number
    with lock:
        number += 1
        return number
    
