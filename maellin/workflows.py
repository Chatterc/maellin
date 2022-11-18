#   Copyright (C) 2022  Carl Chatterton. All Rights Reserved.
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <https://www.gnu.org/licenses/>.

import cloudpickle as cpickle
from maellin.scheduler import DefaultScheduler
from maellin.logger import LoggingMixin
from maellin.graphs import DAG
from maellin.queues import QueueFactory
from maellin.tasks import Task, create_task
from maellin.executors.default import DefaultExecutor
from maellin.exceptions import DependencyError, NotFoundError
from typing import Any, List, Literal, Tuple


class Pipeline(DAG, LoggingMixin):
    """A Directed Acyclic MultiGraph based Pipeline for Data Processing. """

    pipeline_id = 0

    def __init__(
            self,
            steps: List[Task] = [],
            type: Literal['default', 'asyncio', 'multi-threading', 'multi-processing'] = 'default'):

        Pipeline.pipeline_id += 1
        super().__init__()
        self.pid = Pipeline.pipeline_id
        self.steps = [step if isinstance(step, Pipeline) else create_task(step) for step in steps]
        self.type = type
        self._log = self.logger
        self.queue = QueueFactory.factory(type=type)
        self.sched = DefaultScheduler()

    def _merge_dags(self, pipeline: "Pipeline") -> None:
        """Allow a Pipeline object to receive an other Pipeline object \
        by merging two Graphs together and preserving attributes.
        """
        pipeline.compose(self)
        G = pipeline.dag
        self.dag = self.merge(G, self.dag)
        self.repair_attributes(G, self.dag, 'tasks')

    def _proc_pipeline_dep(self, idx, task, dep):
        """Process Dependencies that contain another Pipeline
        """
        # gets the last step from the pipeline dependency
        dep_task = dep.steps[-1]

        # Validate task is compatible with the dependency
        if not task.skip_validation:
            task.validate(dep_task)

        # Update the task with uuids of related runs
        if dep.dag.nodes[dep_task.tid].get('tasks', None) is not None:
            for k in dep.dag.nodes[dep_task.tid]['tasks'].keys():
                task.related.append(k)
        else:
            raise DependencyError(f'{dep} was not found in {self.__name__}, check pipeline steps.')

        # Replace the Pipeline References with Task Reference
        task.depends_on[idx] = dep_task

        return (task, dep_task)

    def _proc_named_dep(self, idx: int, task: Task, dep: str, input_pipe: "Pipeline"):
        """Process Dependencies that contain a reference to another task"""
        # this is very ugly and needs to be refactored
        try:
            dep_task = self.get_task_by_name(name=dep)
            dag = self.dag
        except BaseException:
            dep_task = input_pipe.get_task_by_name(name=dep)
            dag = input_pipe.dag

        task.depends_on[idx] = dep_task

        # Validate task is compatible with the dependency
        if not task.skip_validation:
            task.validate(dep_task)
            self._log.info('Validation Check Complete for %s & %s' % (task.name, dep_task.name))

        # Lookup dependent task from the current pipeline or the called pipeline
        if dag.nodes[dep_task.tid].get('tasks', None) is not None:
            for k in dag.nodes[dep_task.tid]['tasks'].keys():
                task.related.append(k)
        else:
            raise DependencyError(f'{dep_task} was not found in {self.__name__}, check pipeline steps.')

        return (task, dep_task)

    def _proc_task_dep(self, task, dep, input_pipe):
        """Processes Dependencies that contain a subclass of a Task."""

        # Validate task is compatible with the dependency
        if not task.skip_validation:
            task.validate(dep)

        # Lookup dependent task from the current pipeline
        pipe = self
        if dep.tid not in self.dag:
            pipe = input_pipe
        if pipe.dag.nodes[dep.tid].get('tasks', None) is not None:
            for k in pipe.dag.nodes[dep.tid]['tasks'].keys():
                for tsk in pipe.steps:
                    if k == tsk.tid:
                        task.related.append(k)
        else:
            raise DependencyError(f'{dep} was not found in {self.__name__}, check pipeline steps.')

        return (task, dep)

    def _process_dep(self, idx: int, task: Task, dep: Any, input_pipe: "Pipeline") -> Tuple[Task, Task]:
        """Basic Factory function for processing dependencies.
        """
        if isinstance(dep, Pipeline):
            return self._proc_pipeline_dep(idx, task, dep)
        elif isinstance(dep, str):
            return self._proc_named_dep(idx, task, dep, input_pipe)
        elif issubclass(type(dep), Task):
            return self._proc_task_dep(task, dep, input_pipe)
        else:
            raise TypeError("Invalid Dependencies found in {self.__name__}: Task {task.__name__} ")

    def dump(self, filename: str, protocol: str = None):
        """Serializes a DAG using cloudpickle

        Args:
            filename (str): name of file to write steam to.
            protocol (str, optional): protocol defaults to cloudpickle.DEFAULT_PROTOCOL  \
                which is an alias to pickle.HIGHEST_PROTOCOL.

        Usage:
        >>> filename = 'my_dag.pkl' # filename to use to serialize DAG
        >>> pipe = Pipeline(steps=my_steps) # create new pipeline instance with steps
        >>> pipe.compose() # compose a DAG from steps
        >>> pipe.dump(filename) # save the DAG
        """

        with open(filename, 'wb') as f:
            import maellin
            cpickle.register_pickle_by_value(module=maellin)
            cpickle.dump(obj=self.dag, file=f, protocol=protocol)
        return self
    
    def dumps(self, protocol: str = None):
        """Serializes a DAG using cloudpickle"""
        import maellin
        cpickle.register_pickle_by_value(module=maellin)
        pkl_dag = str(cpickle.dumps(obj=self.dag, protocol=protocol)).encode('utf-8')
        return pkl_dag

    def load(self, filename: str):
        """loads a DAG to a pipeline instance

        Args:
            filename (str): filename of the pickled DAG

        Usage:
        >>> filename = 'my_dag.pkl' # filename to pickled dag file
        >>> new_pipe = Pipeline() # create new pipeline instance
        >>> new_pipe.load(filename) # load the dag to the pipeline instance
        >>> new_pipe.run() # run the pipeline
        """

        with open(filename, 'rb') as f:
            dag = cpickle.load(f)
            self.dag = dag
        return self
    
    def loads(self, filename: str):
        """loads a DAG to a pipeline instance

        Args:
            filename (str): filename of the pickled DAG

        Usage:
        >>> filename = 'my_dag.pkl' # filename to pickled dag file
        >>> new_pipe = Pipeline() # create new pipeline instance
        >>> new_pipe.load(filename) # load the dag to the pipeline instance
        >>> new_pipe.run() # run the pipeline
        """

        with open(filename, 'rb') as f:
            dag = cpickle.loads(f)
            self.dag = dag
        return self

    def print_plan(self):
        """Pretty Prints the DAG in Queue Processing Order"""

        from pprint import pformat

        nodes = self.get_all_nodes().copy()
        index_map = {v: i for i, v in enumerate(self.topological_sort())}
        return pformat(dict(sorted(nodes.items(), key=lambda pair: index_map[pair[0]])), compact=True, width=41)

    def get_task_by_name(self, name: str) -> Task:
        """Retrieves an Task from the DAG using its name

        Args:
            name (str): The name of the Task

        Raises:
            NotFoundError: Complains if the task could not be found

        Returns:
            Task: The task that matches the name parameter.
        """
        for tsk_attrs in self.get_all_attributes(name='tasks'):
            if tsk_attrs is not None:
                for tsk in tsk_attrs.values():
                    if tsk.name == name:
                        return tsk

        raise NotFoundError(f"{name} was not found in the DAG")

    def compose(self, input_pipe: "Pipeline" = None) -> None:
        """
        Compose the DAG from steps provided to the pipeline
        """
        # For each task found in steps
        for task in self.steps:
            # Process the task with a special call if it is a Pipeline Instance
            if isinstance(task, Pipeline):
                self._merge_dags(task)
                continue

            # Process the dependencies
            if task.depends_on is not None:
                # Sanity check what was provided as a dependency
                assert isinstance(task.depends_on, List), TypeError(
                    f'Dependencies for Task: {task.name} must be a list.')
                assert len(task.depends_on) >= 1, ValueError(
                    f"No Dependencies Provided for Task: {task.name}")

                for idx, dep_task in enumerate(task.depends_on):
                    assert task != dep_task, DependencyError(f'{task} cannot be \
                        both the active task and dependency')

                    # Process the dependency
                    task, dep_task = self._process_dep(idx, task, dep_task, input_pipe)

                    # Add edge to DAG using task id as an edge key
                    self.add_edge_to_dag(self.pid, dep_task.tid, task.tid, task.tid)

            # Add Task to node with related keys
            task.related = list(dict.fromkeys(task.related).keys())
            self.add_node_to_dag(task)

        # Validates DAG was constructed properly
        self._validate_dag()

    def collect(self) -> None:
        """Enqueues all Tasks from the constructed DAG in topological sort order
        """
        # Compile steps into the DAG if not already compiled
        if self.is_empty():
            self.compose()

        self.queue = QueueFactory.factory(self.type)
        # Begin Enqueuing all Tasks in the DAG
        nodes = self.get_all_nodes()
        # Get Topological sort of Task Nodes by Id
        for task_node_id in self.topological_sort():
            # Lookup each task in a node
            n_attrs = nodes[task_node_id]
            # Enqueue Tasks & update status
            for v in n_attrs['tasks'].values():
                self.queue.put(v)
                v.update_status('Queued')

    def run(self) -> Any:
        """Allows for Local Execution of a Pipeline Instance. Good for Debugging
        for advanced features and concurrency support use submit"""
        self.result_queue = QueueFactory.factory(self.type)
        # If Queue is empty, populate it
        if self.queue.empty():
            self.collect()

        # Setup Default Executor
        executor = DefaultExecutor(
            task_queue=self.queue,
            result_queue=self.result_queue)


        # Start execution of Tasks
        self._log.info('Starting Execution')
        executor.start()
        executor.end()
        
    def submit(
        self,
        name:str,
        trigger='interval',
        minutes=1,
        max_instances=1,
        replace_existing=True) -> str:
        """Submits DAG to the Scheduler"""
        
        if self.sched.state == 0:
            self.sched.start()
       
        self.sched.add_job(
            func=self.run,
            name=name,
            trigger=trigger, 
            replace_existing=replace_existing,
            max_instances=max_instances,
            minutes=minutes)
    