import sys
import copy
import logging
import threading
import heapq
import time
import traceback
import inspect
from typing import List, Literal, NamedTuple, Optional

import torch
import nodes

import comfy.model_management
import execution
from server import PromptServer



class PromptExecutor:
    def __init__(self, server: PromptServer):
        self.server = server
        self.reset()

    def reset(self):
        # Dict {workflow_name1:{}, workflow_name2:{}}
        self.outputs = {}
        self.object_storages = {}
        self.old_prompts = {}
        self.cleanup_lock = threading.Lock()
        self.locks = {}

        self.outputs_ui = {} # Not use

        self.status_messages = {} # promptid to message list
        self.success = {} # promptid to message list
        self.history_profile = {}
    
    def clean_end_execution(self, prompt_id):
        self.status_messages.pop(prompt_id)
        self.success.pop(prompt_id)
        

    def add_message(self, event, data, broadcast: bool, client_id):
        prompt_id = data["prompt_id"]
        if prompt_id not in self.status_messages:
            self.status_messages[prompt_id] = []
        self.status_messages[prompt_id].append((event, data))
        if client_id is not None or broadcast:
            self.parallel_send_sync(prompt_id, event, data, client_id)

    def handle_execution_error(self, prompt_id, prompt, current_outputs, executed, error, ex, client_id, outputs, workflow_old_prompt):
        node_id = error["node_id"]
        class_type = prompt[node_id]["class_type"]

        # First, send back the status to the frontend depending
        # on the exception type
        if isinstance(ex, comfy.model_management.InterruptProcessingException):
            mes = {
                "prompt_id": prompt_id,
                "node_id": node_id,
                "node_type": class_type,
                "executed": list(executed),
            }
            self.add_message("execution_interrupted", mes, broadcast=False, client_id=client_id)
        else:
            mes = {
                "prompt_id": prompt_id,
                "node_id": node_id,
                "node_type": class_type,
                "executed": list(executed),

                "exception_message": error["exception_message"],
                "exception_type": error["exception_type"],
                "traceback": error["traceback"],
                "current_inputs": error["current_inputs"],
                "current_outputs": error["current_outputs"],
            }
            self.add_message("execution_error", mes, broadcast=False, client_id=client_id)
        
        # Next, remove the subsequent outputs since they will not be executed
        to_delete = []
        for o in outputs:
            if (o not in current_outputs) and (o not in executed):
                to_delete += [o]
                if o in workflow_old_prompt:
                    d = workflow_old_prompt.pop(o)
                    del d
        for o in to_delete:
            d = outputs.pop(o)
            del d

    def execute(self, prompt, prompt_id, extra_data={}, execute_outputs=[]):
        nodes.interrupt_processing(False)

        client_id = extra_data["client_id"]
        workflow_name = extra_data["workflow_name"]
        logging.info(f"execute {workflow_name} {prompt_id} {client_id}")

        if workflow_name not in self.outputs:
            self.outputs[workflow_name] = {}
            self.object_storages[workflow_name] = {}
            self.old_prompts[workflow_name] = {}
            self.locks[workflow_name] = {}
            self.locks[workflow_name]["workflow_lock"] = threading.Lock()
            
        workflow_object_storage = self.object_storages[workflow_name]
        workflow_old_prompt = self.old_prompts[workflow_name]
        workflow_lock = self.locks[workflow_name]

        # self.status_messages = []
        self.add_message("execution_start", { "prompt_id": prompt_id}, broadcast=False, client_id=client_id)

        with torch.inference_mode():
            with self.locks[workflow_name]["workflow_lock"]:
                workflow_outputs = self.outputs[workflow_name]
                #delete cached outputs if nodes don't exist for them
                to_delete = []
                for o in workflow_outputs:
                    if o not in prompt:
                        to_delete += [o]
                for o in to_delete:
                    d = workflow_outputs.pop(o)
                    del d
                to_delete = []
                for o in workflow_object_storage:
                    if o[0] not in prompt:
                        to_delete += [o]
                    else:
                        p = prompt[o[0]]
                        if o[1] != p['class_type']:
                            to_delete += [o]
                for o in to_delete:
                    d = workflow_object_storage.pop(o)
                    del d

                for x in prompt:
                    execution.recursive_output_delete_if_changed(prompt, workflow_old_prompt, workflow_outputs, x)

                current_outputs = set(workflow_outputs.keys())
                # shallow copy current output for recursive_will_execute can do right way
                # and avoid data competition
                prompt_outout = shallow_copy(workflow_outputs) 

            with self.cleanup_lock:
                comfy.model_management.cleanup_models(keep_clone_weights_loaded=True)

            self.add_message("execution_cached",
                          { "nodes": list(current_outputs) , "prompt_id": prompt_id},
                          broadcast=False, client_id=client_id,)
            executed = set()
            output_node_id = None
            to_execute = []

            for node_id in list(execute_outputs):
                to_execute += [(0, node_id)]

            
            outputs_ui = {}
            while len(to_execute) > 0:
                #always execute the output that depends on the least amount of unexecuted nodes first
                memo = {}
                to_execute = sorted(list(map(lambda a: (len(execution.recursive_will_execute(prompt, prompt_outout, a[-1], memo)), a[-1]), to_execute)))
                output_node_id = to_execute.pop(0)[-1]

                # This call shouldn't raise anything if there's an error deep in
                # the actual SD code, instead it will report the node where the
                # error was raised
                success, error, ex = pipeline_parallel_recursive_execute(self, prompt, prompt_outout, output_node_id, extra_data, executed, prompt_id,
                                                                         outputs_ui, object_storage=workflow_object_storage, workflow_lock=workflow_lock, CURRENT_START_EXECUTION_DATA = self.history_profile[prompt_id])
                self.success[prompt_id] = success
                if not success:
                    self.handle_execution_error(prompt_id, prompt, current_outputs, executed, error, ex, client_id, workflow_outputs, workflow_old_prompt)
                    break

            with self.locks[workflow_name]["workflow_lock"]:
                self.outputs[workflow_name] = prompt_outout
            
            for x in executed:
                workflow_old_prompt[x] = copy.deepcopy(prompt[x])
            # self.server.last_node_id = None
            if comfy.model_management.DISABLE_SMART_MEMORY:
                with self.cleanup_lock:
                    comfy.model_management.unload_all_models()
        return prompt_outout, outputs_ui


    def parallel_send_sync(self, prompt_id, event, data, sid=None):
        # print(f"{prompt_id} event: {event}, data: {data}")
        if prompt_id not in self.history_profile:
            self.history_profile[prompt_id] = {}
        CURRENT_START_EXECUTION_DATA = self.history_profile[prompt_id]

        if event == "execution_start":
            CURRENT_START_EXECUTION_DATA = dict(
                start_perf_time=time.perf_counter(),
                nodes_start_perf_time={}
            )
        self.server.send_sync(event=event, data=data, sid=sid)

        if event == "executing" and data and CURRENT_START_EXECUTION_DATA:
            if data.get("node") is not None:
                node_id = data.get("node")
                CURRENT_START_EXECUTION_DATA['nodes_start_perf_time'][node_id] = time.perf_counter()
        self.history_profile[prompt_id] = CURRENT_START_EXECUTION_DATA


MAXIMUM_HISTORY_SIZE = 10000

class PromptQueue:
    def __init__(self):
        # self.server = server
        self.mutex = threading.RLock()
        self.not_empty = threading.Condition(self.mutex)
        self.task_counter = 0
        self.workflow_queue = {} #  workflow to list
        self.currently_running = {}
        self.history = {}
        self.flags = {}
        # server.prompt_queue = self

    def put(self, workflow_name, item):
        with self.mutex:
            if workflow_name not in self.workflow_queue:
                self.workflow_queue[workflow_name] = []
            queue = self.workflow_queue[workflow_name]
            heapq.heappush(queue, item)
            # self.server.queue_updated()
            self.not_empty.notify()

    def get(self, timeout=None):
        with self.not_empty:
            for workflow_name, queue in self.workflow_queue.items():
                # queue = self.workflow_queue[workflow_name]
                while len(queue) == 0:
                    self.not_empty.wait(timeout=timeout)
                    if timeout is not None and len(queue) == 0:
                        return None
                item = heapq.heappop(queue)
                i = self.task_counter
                self.currently_running[i] = copy.deepcopy(item)
                self.task_counter += 1
                # self.server.queue_updated()
                return (item, i)

    class ExecutionStatus(NamedTuple):
        status_str: Literal['success', 'error']
        completed: bool
        messages: List[str]

    def task_done(self, item_id, outputs,
                  status: Optional['PromptQueue.ExecutionStatus']):
        with self.mutex:
            prompt = self.currently_running.pop(item_id)
            if len(self.history) > MAXIMUM_HISTORY_SIZE:
                self.history.pop(next(iter(self.history)))

            status_dict: Optional[dict] = None
            if status is not None:
                status_dict = copy.deepcopy(status._asdict())

            self.history[prompt[1]] = {
                "prompt": prompt,
                "outputs": copy.deepcopy(outputs),
                'status': status_dict,
            }
            # self.server.queue_updated()

    # def get_current_queue(self):
    #     pass

    # def get_tasks_remaining(self):
    #     pass

    # def wipe_queue(self):
    #    pass

    # def delete_queue_item(self, function):
    #     pass

    def get_history(self, prompt_id=None, max_items=None, offset=-1):
        with self.mutex:
            if prompt_id is None:
                out = {}
                i = 0
                if offset < 0 and max_items is not None:
                    offset = len(self.history) - max_items
                for k in self.history:
                    if i >= offset:
                        out[k] = self.history[k]
                        if max_items is not None and len(out) >= max_items:
                            break
                    i += 1
                return out
            elif prompt_id in self.history:
                return {prompt_id: copy.deepcopy(self.history[prompt_id])}
            else:
                return {}

    def wipe_history(self):
        with self.mutex:
            self.history = {}

    def delete_history_item(self, id_to_delete):
        with self.mutex:
            self.history.pop(id_to_delete, None)

    def set_flag(self, name, data):
        with self.mutex:
            self.flags[name] = data
            self.not_empty.notify()

    def get_flags(self, reset=True):
        with self.mutex:
            if reset:
                ret = self.flags
                self.flags = {}
                return ret
            else:
                return self.flags.copy()

parallel_prompt_queue = PromptQueue()

    

def pipeline_parallel_recursive_execute(executor :PromptExecutor, prompt, outputs, current_item, extra_data, executed, 
                                        prompt_id, outputs_ui, object_storage, workflow_lock, CURRENT_START_EXECUTION_DATA):
    client_id = extra_data["client_id"]

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
                result = pipeline_parallel_recursive_execute(executor, prompt, outputs, input_unique_id, extra_data, executed, prompt_id,
                                                             outputs_ui, object_storage=object_storage, workflow_lock=workflow_lock, CURRENT_START_EXECUTION_DATA=CURRENT_START_EXECUTION_DATA)
                if result[0] is not True:
                    # Another node failed further upstream
                    return result

    input_data_all = None
    try:
        if current_item not in workflow_lock:
            workflow_lock[current_item] = threading.Lock()
        with workflow_lock[current_item]:
            input_data_all = execution.get_input_data(inputs, class_def, unique_id, outputs, prompt, extra_data)
            if client_id is not None:
                executor.parallel_send_sync(prompt_id, "executing", { "node": unique_id, "prompt_id": prompt_id }, client_id)

            obj = object_storage.get((unique_id, class_type), None)
            if obj is None:
                obj = class_def()
                object_storage[(unique_id, class_type)] = obj

            output_data, output_ui = execution.get_output_data(obj, input_data_all)
            outputs[unique_id] = output_data
        if len(output_ui) > 0:
            outputs_ui[unique_id] = output_ui
            if client_id is not None:
                executor.parallel_send_sync(prompt_id, "executed", { "node": unique_id, "output": output_ui, "prompt_id": prompt_id }, client_id)
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
    if CURRENT_START_EXECUTION_DATA:
        start_time = CURRENT_START_EXECUTION_DATA['nodes_start_perf_time'].get(unique_id)
        if start_time:
            end_time = time.perf_counter()
            execution_time = end_time - start_time
            logging.info(f"{prompt_id} #{unique_id} [{class_type}]: {execution_time:.2f}s")

    return (True, None, None)

def shallow_copy(x: dict):
    c = {}
    for k,v in x.items():
        c[k] = v
    return c





