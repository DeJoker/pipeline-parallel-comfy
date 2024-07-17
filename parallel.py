import gc
import json
import logging
import queue
import sys
import threading
import time
import traceback
import uuid
from aiohttp import web
from concurrent.futures import ThreadPoolExecutor, Future



from server import PromptServer
import execution
from . import parallel_execution
import nodes
import comfy.model_management

routes = PromptServer.instance.routes
server_instance = PromptServer.instance

def execute_hook(e: parallel_execution.PromptExecutor, prompt, prompt_id, extra_data={}, execute_outputs=[]):
    execution_start_time = time.perf_counter()
    prompt_outout, outputs_ui = e.execute(prompt, prompt_id, extra_data, execute_outputs)
    current_time = time.perf_counter()
    execution_time = current_time - execution_start_time
    logging.info("Prompt executed in {:.2f} seconds".format(execution_time))
    return prompt_outout, outputs_ui
    

def prompt_worker(q: parallel_execution.PromptQueue, server: PromptServer):
    server.last_prompt_id = '' # add PromptServer attribute when UI or /prompt not do it
    parallelExecutor = ThreadPoolExecutor(max_workers=6)

    e = parallel_execution.PromptExecutor(server)
    last_gc_collect = 0
    need_gc = False
    gc_collect_interval = 10.0

    while True:
        timeout = 0.1
        if need_gc:
            timeout = max(gc_collect_interval - (current_time - last_gc_collect), 0.1)

        if len(q.currently_running) < parallelExecutor._max_workers: # wait queue is empty free to execute
            queue_item = q.get(timeout=timeout)
            if queue_item is not None: 
                item, item_id = queue_item
                prompt_id = item[1]

                first_workflow_prompt = True
                workflow_name = item[3]["workflow_name"]
                if workflow_name in e.outputs:
                    first_workflow_prompt = False

                future = parallelExecutor.submit(execute_hook, e, item[2], prompt_id, item[3], item[4])
            
                def done_cb(_future: Future, prompt_id=item[1], extra_data = item[3], item_id=item_id):
                    prompt_outout, outputs_ui = _future.result()
                    logging.info(f"done_cb {prompt_id}")
                    q.task_done(item_id,
                                outputs_ui,
                                status=parallel_execution.PromptQueue.ExecutionStatus(
                                    status_str='success' if e.success else 'error',
                                    completed=e.success,
                                    messages=e.status_messages))
                    if extra_data.get("client_id") is not None:
                        server.send_sync("executing", { "node": None, "prompt_id": prompt_id }, extra_data.get("client_id"))
                future.add_done_callback(done_cb)
                if first_workflow_prompt:
                    future.result() # first prompt wait for it
            
                logging.info(f"currently_running:{len(q.currently_running)}")

        if len(q.currently_running) == 0:
            need_gc = True
            flags = q.get_flags()
            free_memory = flags.get("free_memory", False)

            if flags.get("unload_models", free_memory):
                comfy.model_management.unload_all_models()
                need_gc = True
                last_gc_collect = 0

            if free_memory:
                e.reset()
                need_gc = True
                last_gc_collect = 0

            if need_gc:
                current_time = time.perf_counter()
                if (current_time - last_gc_collect) > gc_collect_interval:
                    comfy.model_management.cleanup_models()
                    gc.collect()
                    comfy.model_management.soft_empty_cache()
                    last_gc_collect = current_time
                    need_gc = False


threading.Thread(target=prompt_worker, 
                    args=(parallel_execution.parallel_prompt_queue, PromptServer.instance,),
                    daemon=True, 
                ).start()



class PipelineParallel:
    def __init__(self):
        self.type = "output"

    @classmethod
    def INPUT_TYPES(self):
        return {
            "required": {
                "executor_count": ("INT", )
            },
            "optional": {
            }
        }

    RETURN_TYPES = ()
    RETURN_NAMES = ()
    FUNCTION = 'set_parallel_config'
    CATEGORY = 'parallel'
    OUTPUT_NODE = True

    def set_parallel_config(self, executor_count):
        return { "ui": { "executor_count": [executor_count] } }
    
NODE_CLASS_MAPPINGS = {
    "PipelineParallel": PipelineParallel
}
