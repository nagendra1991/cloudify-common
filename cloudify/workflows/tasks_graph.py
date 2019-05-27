########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.


import time
import Queue
from functools import wraps
from collections import deque


import networkx as nx

from cloudify.workflows import api
from cloudify.workflows import tasks
from cloudify.state import workflow_ctx


def make_or_get_graph(f):
    """Decorate a graph-creating function with this, to automatically
    make it try to retrieve the graph from storage first.
    """
    @wraps(f)
    def _inner(*args, **kwargs):
        if workflow_ctx.dry_run:
            kwargs.pop('name', None)
            return f(*args, **kwargs)
        name = kwargs.pop('name')
        graph = workflow_ctx.get_tasks_graph(name)
        if not graph:
            graph = f(*args, **kwargs)
            graph.store(name=name)
        else:
            graph = TaskDependencyGraph.restore(workflow_ctx, graph)
        return graph
    return _inner


class GraphNode(object):
    def __init__(self, task):
        self._task = task
        self._dependencies = set()
        self._dependents = set()

    @property
    def id(self):
        return self._task.id

    def __hash__(self):
        return hash(self._task.id)

    def __eq__(self, other):
        return self.id == other.id


class RootNode(GraphNode):
    def __init__(self):
        super(RootNode, self).__init__(None)

    @property
    def id(self):
        return None

    def __hash__(self):
        return 0


class TaskDependencyGraph(object):
    """
    A task graph builder

    :param workflow_context: A WorkflowContext instance (used for logging)
    """

    @classmethod
    def restore(cls, workflow_context, retrieved_graph):
        graph = cls(workflow_context, graph_id=retrieved_graph.id)
        operations = workflow_context.get_operations(retrieved_graph.id)
        ops = {}
        ctx = workflow_context._get_current_object()
        for op_descr in operations:
            if op_descr.state in tasks.TERMINATED_STATES:
                continue
            op = OP_TYPES[op_descr.type].restore(ctx, graph, op_descr)
            ops[op_descr.id] = op

        for op in ops.values():
            if op.containing_subgraph:
                subgraph_id = op.containing_subgraph
                op.containing_subgraph = None
                subgraph = ops[subgraph_id]
                subgraph.add_task(op)
            else:
                graph.add_task(op)

        for op_descr in operations:
            op = ops.get(op_descr.id)
            if op is None:
                continue
            for target in op_descr.dependencies:
                if target not in ops:
                    continue
                target = ops[target]
                graph.add_dependency(op, target)

        graph._stored = True
        return graph

    def __init__(self, workflow_context, graph_id=None,
                 default_subgraph_task_config=None):
        self.ctx = workflow_context
        self.graph = nx.DiGraph()
        default_subgraph_task_config = default_subgraph_task_config or {}
        self._default_subgraph_task_config = default_subgraph_task_config
        self._error = None
        self._stored = False
        self.id = graph_id
        self._root_node = RootNode()
        self._tasks = {None: self._root_node}

    def store(self, name):
        serialized_tasks = []
        for task in self.tasks_iter():
            serialized = task.dump()
            serialized['dependencies'] = list(
                self.graph.succ.get(task.id, {}).keys())
            serialized_tasks.append(serialized)
        stored_graph = self.ctx.store_tasks_graph(
            name, operations=serialized_tasks)
        if stored_graph:
            self.id = stored_graph['id']
            self._stored = True

    def add_task(self, task):
        """Add a WorkflowTask to this graph

        :param task: The task
        """
        self._tasks[task.id] = GraphNode(task)
        self.add_dependency(task, self._root_node)

    def get_task(self, task_id):
        """Get a task instance that was inserted to this graph by its id

        :param task_id: the task id
        :return: a WorkflowTask instance for the requested task if found.
                 None, otherwise.
        """
        data = self._tasks.get(task_id)
        return data._task if data is not None else None

    def remove_task(self, task):
        """Remove the provided task from the graph

        :param task: The task
        """
        raise NotImplementedError()
        if task.is_subgraph:
            for subgraph_task in task.tasks.values():
                self.remove_task(subgraph_task)
        if task.id in self.graph:
            self.graph.remove_node(task.id)

    # src depends on dst
    def add_dependency(self, src_task, dst_task):
        """
        Add a dependency between tasks.
        The source task will only be executed after the target task terminates.
        A task may depend on several tasks, in which case it will only be
        executed after all its 'destination' tasks terminate

        :param src_task: The source task
        :param dst_task: The target task
        """
        if src_task.id not in self._tasks:
            raise RuntimeError('source task {0} is not in graph (task id: '
                               '{1})'.format(src_task, src_task.id))
        if dst_task.id not in self._tasks:
            raise RuntimeError('destination task {0} is not in graph (task '
                               'id: {1})'.format(dst_task, dst_task.id))
        self._tasks[dst_task.id]._dependents.add(self._tasks[src_task.id])
        self._tasks[src_task.id]._dependencies.add(self._tasks[dst_task.id])
        if dst_task.id is not None \
                and self._root_node in self._tasks[src_task.id]._dependencies:
            self._tasks[src_task.id]._dependencies.remove(self._root_node)
            self._root_node._dependents.remove(self._tasks[src_task.id])

    def sequence(self):
        """
        :return: a new TaskSequence for this graph
        """
        return TaskSequence(self)

    def subgraph(self, name):
        task = SubgraphTask(self, info=name,
                            **self._default_subgraph_task_config)
        self.add_task(task)
        return task

    def execute(self):
        """
        Start executing the graph based on tasks and dependencies between
        them.\
        Calling this method will block until one of the following occurs:\
            1. all tasks terminated\
            2. a task failed\
            3. an unhandled exception is raised\
            4. the execution is cancelled\

        Note: This method will raise an api.ExecutionCancelled error if the\
        execution has been cancelled. When catching errors raised from this\
        method, make sure to re-raise the error if it's\
        api.ExecutionsCancelled in order to allow the execution to be set in\
        cancelled mode properly.\

        Also note that for the time being, if such a cancelling event\
        occurs, the method might return even while there's some operations\
        still being executed.
        """
        # clear error, in case the tasks graph has been reused
        self._error = None

        waiting = 0
        current = set(self._root_node._dependents)
        queue = Queue.Queue()
        while current or waiting:
            while current:
                node = current.pop()
                result = node._task.apply_async()
                result.add_callback(lambda *a: queue.put(a), node)
                waiting += 1
            _, node = queue.get()
            waiting -= 1
            for dependent in node._dependents:
                current.add(dependent)

    @staticmethod
    def _is_execution_cancelled():
        return api.has_cancel_request()

    def _executable_tasks(self):
        """
        A task is executable if it is in pending state
        , it has no dependencies at the moment (i.e. all of its dependencies
        already terminated) and its execution timestamp is smaller then the
        current timestamp

        :return: An iterator for executable tasks
        """
        now = time.time()
        return (task for task in self.tasks_iter()
                if (task.get_state() == tasks.TASK_PENDING or
                    task._should_resume()) and
                task.execute_after <= now and
                not (task.containing_subgraph and
                     task.containing_subgraph.get_state() ==
                     tasks.TASK_FAILED) and
                not self._task_has_dependencies(task))

    def _terminated_tasks(self):
        """
        A task is terminated if it is in 'succeeded' or 'failed' state

        :return: An iterator for terminated tasks
        """
        return (task for task in self.tasks_iter()
                if task.get_state() in tasks.TERMINATED_STATES)

    def _sent_tasks(self):
        """Tasks that are in the 'sent' state"""
        return (task for task in self.tasks_iter()
                if task.get_state() == tasks.TASK_SENT)

    def _task_has_dependencies(self, task):
        """
        :param task: The task
        :return: Does this task have any dependencies
        """
        return (len(self.graph.succ.get(task.id, {})) > 0 or
                (task.containing_subgraph and self._task_has_dependencies(
                    task.containing_subgraph)))

    def tasks_iter(self):
        """An iterator on tasks added to the graph"""
        seen = set([])
        nodes = deque([self._root_node])
        while nodes:
            current = nodes.popleft()
            seen.add(current.id)
            if current is not self._root_node:
                yield current
            nodes += [dependent for dependent in current._dependents
                      if seen.issuperset(dependent._dependencies)]

    def _handle_executable_task(self, task):
        """Handle executable task"""
        task.apply_async()

    def _handle_terminated_task(self, task):
        """Handle terminated task"""
        handler_result = task.handle_task_terminated()

        dependents = self.graph.predecessors(task.id)
        removed_edges = [(dependent, task.id)
                         for dependent in dependents]
        self.graph.remove_edges_from(removed_edges)
        self.graph.remove_node(task.id)
        if handler_result.action == tasks.HandlerResult.HANDLER_FAIL:
            if isinstance(task, SubgraphTask) and task.failed_task:
                task = task.failed_task
            message = "Workflow failed: Task failed '{0}'".format(task.name)
            if task.error:
                message = '{0} -> {1}'.format(message, task.error)
            if self._error is None:
                self._error = RuntimeError(message)
        elif handler_result.action == tasks.HandlerResult.HANDLER_RETRY:
            new_task = handler_result.retried_task
            if self.id is not None:
                self.ctx.store_operation(new_task, dependents, self.id)
                new_task.stored = True
            self.add_task(new_task)
            added_edges = [(dependent, new_task.id)
                           for dependent in dependents]
            self.graph.add_edges_from(added_edges)


class forkjoin(object):  # NOQA
    """
    A simple wrapper for tasks. Used in conjunction with TaskSequence.
    Defined to make the code easier to read (instead of passing a list)
    see ``TaskSequence.add`` for more details
    """

    def __init__(self, *tasks):
        self.tasks = tasks


class TaskSequence(object):
    """
    Helper class to add tasks in a sequential manner to a task dependency
    graph

    :param graph: The TaskDependencyGraph instance
    """

    def __init__(self, graph):
        self.graph = graph
        self.last_fork_join_tasks = None

    def add(self, *tasks):
        """
        Add tasks to the sequence.

        :param tasks: Each task might be:

                      * A WorkflowTask instance, in which case, it will be
                        added to the graph with a dependency between it and
                        the task previously inserted into the sequence
                      * A forkjoin of tasks, in which case it will be treated
                        as a "fork-join" task in the sequence, i.e. all the
                        fork-join tasks will depend on the last task in the
                        sequence (could be fork join) and the next added task
                        will depend on all tasks in this fork-join task
        """
        for fork_join_tasks in tasks:
            if isinstance(fork_join_tasks, forkjoin):
                fork_join_tasks = fork_join_tasks.tasks
            else:
                fork_join_tasks = [fork_join_tasks]
            for task in fork_join_tasks:
                self.graph.add_task(task)
                if self.last_fork_join_tasks is not None:
                    for last_fork_join_task in self.last_fork_join_tasks:
                        self.graph.add_dependency(task, last_fork_join_task)
            if fork_join_tasks:
                self.last_fork_join_tasks = fork_join_tasks


class SubgraphTask(tasks.WorkflowTask):

    def __init__(self,
                 graph,
                 workflow_context=None,
                 task_id=None,
                 on_success=None,
                 on_failure=None,
                 info=None,
                 total_retries=tasks.DEFAULT_SUBGRAPH_TOTAL_RETRIES,
                 retry_interval=tasks.DEFAULT_RETRY_INTERVAL,
                 send_task_events=tasks.DEFAULT_SEND_TASK_EVENTS,
                 **kwargs):
        super(SubgraphTask, self).__init__(
            graph.ctx,
            task_id,
            info=info,
            on_success=on_success,
            on_failure=on_failure,
            total_retries=total_retries,
            retry_interval=retry_interval,
            send_task_events=send_task_events)
        self.graph = graph
        self._name = info
        self.tasks = {}
        self.failed_task = None
        if not self.on_failure:
            self.on_failure = lambda tsk: tasks.HandlerResult.fail()
        self.async_result = tasks.StubAsyncResult()

    @classmethod
    def restore(cls, ctx, graph, task_descr):
        task_descr.parameters['task_kwargs']['graph'] = graph
        return super(SubgraphTask, cls).restore(ctx, graph, task_descr)

    def _duplicate(self):
        raise NotImplementedError('self.retried_task should be set explicitly'
                                  ' in self.on_failure handler')

    @property
    def cloudify_context(self):
        return {}

    def is_local(self):
        return True

    @property
    def name(self):
        return self._name

    @property
    def is_subgraph(self):
        return True

    def sequence(self):
        return TaskSequence(self)

    def subgraph(self, name):
        task = SubgraphTask(self.graph, info=name,
                            **self.graph._default_subgraph_task_config)
        self.add_task(task)
        return task

    def add_task(self, task):
        self.graph.add_task(task)
        self.tasks[task.id] = task
        if task.containing_subgraph and task.containing_subgraph is not self:
            raise RuntimeError('task {0}[{1}] cannot be contained in more '
                               'than one subgraph. It is currently contained '
                               'in {2} and it is now being added to {3}'
                               .format(task,
                                       task.id,
                                       task.containing_subgraph.name,
                                       self.name))
        task.containing_subgraph = self

    def remove_task(self, task):
        self.graph.remove_task(task)

    def add_dependency(self, src_task, dst_task):
        self.graph.add_dependency(src_task, dst_task)

    def apply_async(self):
        if not self.tasks:
            self.set_state(tasks.TASK_SUCCEEDED)
        else:
            self.set_state(tasks.TASK_STARTED)
        return self.async_result

    def task_terminated(self, task, new_task=None):
        del self.tasks[task.id]
        if new_task:
            self.tasks[new_task.id] = new_task
            new_task.containing_subgraph = self
        if not self.tasks and self.get_state() not in tasks.TERMINATED_STATES:
            self.set_state(tasks.TASK_SUCCEEDED)


OP_TYPES = {
    'RemoteWorkflowTask': tasks.RemoteWorkflowTask,
    'LocalWorkflowTask': tasks.LocalWorkflowTask,
    'NOPLocalWorkflowTask': tasks.NOPLocalWorkflowTask,
    'SubgraphTask': SubgraphTask
}
