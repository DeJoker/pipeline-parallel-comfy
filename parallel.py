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
from .utils import PipelineConfig, inc_number, from_origin



from server import PromptServer
import execution
from . import parallel_execution
import nodes
import comfy.model_management

routes = PromptServer.instance.routes
server_instance = PromptServer.instance
parallel_executor = parallel_execution.PromptExecutor(server_instance)
threadExecutor = ThreadPoolExecutor(max_workers=PipelineConfig.threads_count)


def execute_hook(prompt, prompt_id, extra_data={}, execute_outputs=[]):
    execution_start_time = time.perf_counter()
    prompt_outout, outputs_ui = parallel_executor.execute(prompt, prompt_id, extra_data, execute_outputs)
    current_time = time.perf_counter()
    execution_time = current_time - execution_start_time
    logging.info("Prompt executed in {:.2f} seconds".format(execution_time))
    return prompt_outout, outputs_ui

origin_prompt_execute = execution.PromptExecutor.execute

def search_origin_itemid(prompt_id: str):
    origin_q: execution.PromptQueue = server_instance.prompt_queue
    origin_q.task_counter-1

# prompt, prompt_id, extra_data={}, execute_outputs=[]
def run_in_parallel_execute(self, *args, **kwargs):
    print("args:", args)
    print("kwargs:", kwargs)
    # in main.py prompt_worker() func

    item_id = search_origin_itemid()
    parallel_execution.parallel_prompt_queue.put(from_origin, (item_id,)+args)

execution.PromptExecutor.execute = run_in_parallel_execute


def prompt_worker(q: parallel_execution.PromptQueue, server: PromptServer):
    logging.info(f"origin_q {id(server.prompt_queue)}")
    server.last_prompt_id = '' # add PromptServer attribute when UI or /prompt not do it

    last_gc_collect = 0
    need_gc = False
    gc_collect_interval = 10.0

    while True:
        timeout = 1.0
        if need_gc:
            timeout = max(gc_collect_interval - (current_time - last_gc_collect), 0.0)

        if len(q.currently_running) < threadExecutor._max_workers: # wait queue is empty free to execute
            queue_item = q.get(timeout=timeout)
            if queue_item is not None:
                item, item_id = queue_item
                prompt_id = item[2]

                first_workflow_prompt = True
                workflow_name = item[3]["workflow_name"]
                if workflow_name in parallel_executor.outputs:
                    first_workflow_prompt = False

                future = threadExecutor.submit(execute_hook, item[1], prompt_id, item[3], item[4])
            
                def done_cb(_future: Future, workflow_name=workflow_name, prompt_id=prompt_id, extra_data = item[3], item_id=item_id):
                    prompt_outout, outputs_ui = _future.result()
                    logging.info(f"done_cb {prompt_id}")
                    status=parallel_execution.PromptQueue.ExecutionStatus(
                                    status_str='success' if parallel_executor.success else 'error',
                                    completed=parallel_executor.success,
                                    messages=parallel_executor.status_messages)
                    
                    if workflow_name == from_origin:
                        logging.info(f"{item_id} {outputs_ui}")
                        server.prompt_queue.task_done(item_id, outputs_ui, status=status)
                        server.queue_updated()
                    else:
                        q.task_done(item_id, outputs_ui, status=status)
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
                parallel_executor.reset()
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
                    args=(parallel_execution.parallel_prompt_queue, server_instance,),
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
