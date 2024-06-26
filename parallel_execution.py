import sys
import copy
import logging
import threading
import heapq
import traceback
import inspect
from typing import List, Literal, NamedTuple, Optional

import torch
import nodes

import comfy.model_management
import execution

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

def recursive_will_execute(prompt, workflow_outputs, current_item, memo={}):
    unique_id = current_item

    if unique_id in memo:
        return memo[unique_id]

    inputs = prompt[unique_id]['inputs']
    will_execute = []
    if unique_id in workflow_outputs:
        return []

    for x in inputs:
        input_data = inputs[x]
        if isinstance(input_data, list):
            input_unique_id = input_data[0]
            output_index = input_data[1]
            if input_unique_id not in workflow_outputs:
                will_execute += recursive_will_execute(prompt, workflow_outputs, input_unique_id, memo)

    memo[unique_id] = will_execute + [unique_id]
    return memo[unique_id]

def recursive_output_delete_if_changed(prompt, old_prompt, workflow_outputs, current_item):
    unique_id = current_item
    inputs = prompt[unique_id]['inputs']
    class_type = prompt[unique_id]['class_type']
    class_def = nodes.NODE_CLASS_MAPPINGS[class_type]

    is_changed_old = ''
    is_changed = ''
    to_delete = False
    if hasattr(class_def, 'IS_CHANGED'):
        if unique_id in old_prompt and 'is_changed' in old_prompt[unique_id]:
            is_changed_old = old_prompt[unique_id]['is_changed']
        if 'is_changed' not in prompt[unique_id]:
            input_data_all = execution.get_input_data(inputs, class_def, unique_id, workflow_outputs)
            if input_data_all is not None:
                try:
                    #is_changed = class_def.IS_CHANGED(**input_data_all)
                    is_changed = execution.map_node_over_list(class_def, input_data_all, "IS_CHANGED")
                    prompt[unique_id]['is_changed'] = is_changed
                except:
                    to_delete = True
        else:
            is_changed = prompt[unique_id]['is_changed']

    if unique_id not in workflow_outputs:
        return True

    if not to_delete:
        if is_changed != is_changed_old:
            to_delete = True
        elif unique_id not in old_prompt:
            to_delete = True
        elif inputs == old_prompt[unique_id]['inputs']:
            for x in inputs:
                input_data = inputs[x]

                if isinstance(input_data, list):
                    input_unique_id = input_data[0]
                    output_index = input_data[1]
                    if input_unique_id in workflow_outputs:
                        to_delete = recursive_output_delete_if_changed(prompt, old_prompt, workflow_outputs, input_unique_id)
                    else:
                        to_delete = True
                    if to_delete:
                        break
        else:
            to_delete = True

    if to_delete:
        d = workflow_outputs.pop(unique_id)
        del d
    return to_delete

class PromptExecutor:
    def __init__(self, server):
        self.server = server
        self.reset()

    def reset(self):
        # 二维Dict {workflow_name1:{}, workflow_name2:{}}
        self.outputs = {}
        self.object_storage = {}
        self.old_prompts = {}

        # self.outputs_ui = {} # Not use
        # self.status_messages = [] # Not use
        # self.success = True # use local varabile

    def add_message(self, event, data, broadcast: bool, client_id):
        # self.status_messages.append((event, data))
        if client_id is not None or broadcast:
            self.server.send_sync(event, data, client_id)

    def handle_execution_error(self, prompt_id, prompt, current_outputs, executed, error, ex, client_id, outputs):
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
            self.add_message("execution_interrupted", mes, broadcast=True, client_id=client_id)
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
                if o in self.old_prompt:
                    d = self.old_prompt.pop(o)
                    del d
        for o in to_delete:
            d = outputs.pop(o)
            del d

    def execute(self, prompt, prompt_id, extra_data={}, execute_outputs=[]):
        nodes.interrupt_processing(False)

        client_id = extra_data["client_id"]
        workflow_name = extra_data["workflow_name"]

        if workflow_name not in self.outputs:
            self.outputs[workflow_name] = {}
        workflow_outputs = self.outputs[workflow_name]


        # self.status_messages = []
        self.add_message("execution_start", { "prompt_id": prompt_id}, broadcast=False, client_id=client_id)

        with torch.inference_mode():
            #delete cached outputs if nodes don't exist for them
            to_delete = []
            for o in workflow_outputs:
                if o not in prompt:
                    to_delete += [o]
            for o in to_delete:
                d = workflow_outputs.pop(o)
                del d
            to_delete = []
            for o in self.object_storage:
                if o[0] not in prompt:
                    to_delete += [o]
                else:
                    p = prompt[o[0]]
                    if o[1] != p['class_type']:
                        to_delete += [o]
            for o in to_delete:
                d = self.object_storage.pop(o)
                del d

            for x in prompt:
                recursive_output_delete_if_changed(prompt, self.old_prompt, workflow_outputs, x)

            current_outputs = set(workflow_outputs.keys())
            # for x in list(self.outputs_ui.keys()):
            #     if x not in current_outputs:
            #         d = self.outputs_ui.pop(x)
            #         del d

            comfy.model_management.cleanup_models(keep_clone_weights_loaded=True)
            self.add_message("execution_cached",
                          { "nodes": list(current_outputs) , "prompt_id": prompt_id},
                          broadcast=False, client_id=client_id,)
            executed = set()
            output_node_id = None
            to_execute = []

            for node_id in list(execute_outputs):
                to_execute += [(0, node_id)]

            while len(to_execute) > 0:
                #always execute the output that depends on the least amount of unexecuted nodes first
                memo = {}
                to_execute = sorted(list(map(lambda a: (len(recursive_will_execute(prompt, workflow_outputs, a[-1], memo)), a[-1]), to_execute)))
                output_node_id = to_execute.pop(0)[-1]

                # This call shouldn't raise anything if there's an error deep in
                # the actual SD code, instead it will report the node where the
                # error was raised
                success, error, ex = recursive_output_delete_if_changed(self.server, prompt, workflow_outputs, output_node_id, extra_data, executed, prompt_id, self.outputs_ui, self.object_storage)
                if success is not True:
                    self.handle_execution_error(prompt_id, prompt, current_outputs, executed, error, ex, client_id, workflow_outputs)
                    break

            for x in executed:
                self.old_prompt[x] = copy.deepcopy(prompt[x])
            self.server.last_node_id = None
            if comfy.model_management.DISABLE_SMART_MEMORY:
                comfy.model_management.unload_all_models()



MAXIMUM_HISTORY_SIZE = 10000

class PromptQueue:
    def __init__(self, server):
        self.server = server
        self.mutex = threading.RLock()
        self.not_empty = threading.Condition(self.mutex)
        self.task_counter = 0
        self.workflow_queue = {} #  workflow to list
        self.currently_running = {}
        self.history = {}
        self.flags = {}
        server.prompt_queue = self

    def put(self, workflow_name, item):
        with self.mutex:
            queue = self.workflow_queue[workflow_name]
            heapq.heappush(queue, item)
            self.server.queue_updated()
            self.not_empty.notify()

    def get(self, workflow_name, timeout=None):
        with self.not_empty:
            queue = self.workflow_queue[workflow_name]
            while len(queue) == 0:
                self.not_empty.wait(timeout=timeout)
                if timeout is not None and len(queue) == 0:
                    return None
            item = heapq.heappop(queue)
            i = self.task_counter
            self.currently_running[i] = copy.deepcopy(item)
            self.task_counter += 1
            self.server.queue_updated()
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
            self.server.queue_updated()

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
