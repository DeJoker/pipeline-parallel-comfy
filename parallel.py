import gc
import json
import logging
import queue
import sys
import time
import traceback
import uuid
from aiohttp import web
from concurrent.futures import ThreadPoolExecutor, Future

from server import PromptServer
import execution
import nodes
import main
import comfy.model_management

routes = PromptServer.instance.routes
server_instance = PromptServer.instance

# def pipeline_parallel_recursive_execute(server_instance, *args, **kwargs):
def pipeline_parallel_recursive_execute(server, prompt, outputs, current_item, extra_data, executed, prompt_id, outputs_ui, object_storage):
    unique_id = current_item
    inputs = prompt[unique_id]['inputs']
    class_type = prompt[unique_id]['class_type']
    class_def = nodes.NODE_CLASS_MAPPINGS[class_type]
    if unique_id in outputs:
        return (True, None, None)

    for x in inputs:
        input_data = inputs[x]

        if isinstance(input_data, list):
            input_unique_id = input_data[0]
            output_index = input_data[1]
            if input_unique_id not in outputs:
                result = pipeline_parallel_recursive_execute(server, prompt, outputs, input_unique_id, extra_data, executed, prompt_id, outputs_ui, object_storage)
                if result[0] is not True:
                    # Another node failed further upstream
                    return result

    input_data_all = None
    try:
        input_data_all = execution.get_input_data(inputs, class_def, unique_id, outputs, prompt, extra_data)
        if server.client_id is not None:
            server.last_node_id = unique_id
            server.send_sync("executing", { "node": unique_id, "prompt_id": prompt_id }, server.client_id)

        obj = object_storage.get((unique_id, class_type), None)
        if obj is None:
            obj = class_def()
            object_storage[(unique_id, class_type)] = obj

        output_data, output_ui = execution.get_output_data(obj, input_data_all)
        outputs[unique_id] = output_data
        if len(output_ui) > 0:
            outputs_ui[unique_id] = output_ui
            if server.client_id is not None:
                server.send_sync("executed", { "node": unique_id, "output": output_ui, "prompt_id": prompt_id }, server.client_id)
    except comfy.model_management.InterruptProcessingException as iex:
        logging.info("Processing interrupted")

        # skip formatting inputs/outputs
        error_details = {
            "node_id": unique_id,
        }

        return (False, error_details, iex)
    except Exception as ex:
        typ, _, tb = sys.exc_info()
        exception_type = execution.full_type_name(typ)
        input_data_formatted = {}
        if input_data_all is not None:
            input_data_formatted = {}
            for name, inputs in input_data_all.items():
                input_data_formatted[name] = [execution.format_value(x) for x in inputs]

        output_data_formatted = {}
        for node_id, node_outputs in outputs.items():
            output_data_formatted[node_id] = [[execution.format_value(x) for x in l] for l in node_outputs]

        logging.error(f"!!! Exception during processing!!! {ex}")
        logging.error(traceback.format_exc())

        error_details = {
            "node_id": unique_id,
            "exception_message": str(ex),
            "exception_type": exception_type,
            "traceback": traceback.format_tb(tb),
            "current_inputs": input_data_formatted,
            "current_outputs": output_data_formatted
        }
        return (False, error_details, ex)

    executed.add(unique_id)

    return (True, None, None)

def execute_hook(e: execution.PromptExecutor, prompt, prompt_id, extra_data={}, execute_outputs=[]):
    execution_start_time = time.perf_counter()
    e.execute(prompt, prompt_id, extra_data, execute_outputs)
    current_time = time.perf_counter()
    execution_time = current_time - execution_start_time
    logging.info("Prompt executed in {:.2f} seconds".format(execution_time))
    

def prompt_worker(q: execution.PromptQueue, server: PromptServer):
    parallelExecutor = ThreadPoolExecutor(max_worker=12)

    e = execution.PromptExecutor(server)
    last_gc_collect = 0
    need_gc = False
    gc_collect_interval = 10.0

    while True:
        timeout = 1.0
        if need_gc:
            timeout = max(gc_collect_interval - (current_time - last_gc_collect), 0.0)

        queue_item = q.get(timeout=timeout)
        if queue_item is not None and parallelExecutor._work_queue.qsize() == 0:
            item, item_id = queue_item
            prompt_id = item[1]
            server.last_prompt_id = prompt_id

            future = parallelExecutor.submit(execute_hook, e, item[2], prompt_id, item[3], item[4])
        
            def done_cb(_future: Future, extra_data = item[3], item_id=item_id):
                q.task_done(item_id,
                            e.outputs_ui,
                            status=execution.PromptQueue.ExecutionStatus(
                                status_str='success' if e.success else 'error',
                                completed=e.success,
                                messages=e.status_messages))
                if extra_data.get("client_id") is not None:
                    server.send_sync("executing", { "node": None, "prompt_id": prompt_id }, extra_data.get("client_id"))
            future.add_done_callback(done_cb)

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



# execution.pipeline_parallel_recursive_execute = execution.PromptExecutor.recursive_execute
execution.recursive_execute = pipeline_parallel_recursive_execute

main_prompt_worker = main.prompt_worker
main.prompt_worker = prompt_worker

