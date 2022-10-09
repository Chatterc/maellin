from typing import Any, Callable, Dict, List, Literal, Tuple, Type, Union

import cloudpickle as cpickle
from cne.pipelines.collector import GarbageCollector
from cne.pipelines.graph import DAG
from cne.pipelines.queues import QueueFactory
from cne.pipelines.tasks import Task
from cne.pipelines.utils import (ActivityFailedError, DependencyError,
                                 NotFoundError, generate_uuid)


def create_task(task: Union[Type[Task], Tuple[Callable, str]]):
    if isinstance(task, Task):
        return task
    elif isinstance(task, Pipeline):
        return task
    task = Task(task)
    return task


class Activity:

    def __init__(
            self,
            task: Type[Task],
            kwargs: Dict = {},
            depends_on: List = None,
            skip_validation: bool = False,
            concurrency: int = 0,
            retry: int = 0,
            timeout: int = 0,
            name: str = None,
            desc: str = None) -> None:
        """Activities are used to define how a task should be performed in a data pipeline.

        Args:
            task (Type[Task]): A Task instance
            kwargs (dict, optional): Dictionary of keyword arguments \
                that are provided to the Task instance. Defaults to None.
            depends_on (List, optional): List of dependencies. Defaults to None.
            skip_validation (boolean): Skips Task validation when set to True. Defaults to False.
            concurrency (int, optional): Sets level of concurrency. Defaults to 0.
            retry (int, optional): number of retries. Defaults to 0.
            timeout (int, optional): amount of time to wait before abandoning the task. Defaults to 0.
            name (str, optional): Name of Activity. Defaults to None.
            desc (str, optional): Description of Activity. Defaults to None.
        """
        self.id = generate_uuid()
        self.name = name
        self.desc = desc
        self.status = "Not Started"
        self.concurrency = concurrency
        self.retry = retry
        self.timeout = timeout
        self.skip_validation = skip_validation
        self.task = create_task(task)
        self.kwargs = kwargs
        self.depends_on = depends_on
        self.related = []
        self.result = None

    def __str__(self) -> str:
        from pprint import pprint
        s = dict()
        s['Activity'] = self.__dict__.copy()
        return str(pprint(s))

    def __repr__(self) -> str:
        return "<class '{}({})>'".format(
            self.__class__.__name__,
            ''.join('{}={!r}, '.format(k, v) for k, v in self.__dict__.items())
        )

    def update_status(self, status: Literal['Not Started', 'Queued', 'Running',
                      'Completed', 'Failed'] = 'Not Started') -> None:
        """Updates the Status of an Activity throughout processing"""
        self.status = status

    def run(self, inputs: Tuple = ()):
        self.result = self.task.run(*inputs, **self.kwargs)


def create_activity(input: Union[Type[Activity], Tuple]):
    if isinstance(input, Activity):
        return input
    activity = Activity(*input)
    return activity


class Pipeline(DAG):

    pipeline_id = 0

    def __init__(
            self,
            steps: List[Type[Activity]] = [],
            type: Literal['basic', 'async'] = 'basic',
            gc_enabled: bool = True):
        """A Directed Acyclic Graph based Pipeline for Data Processing. \n
        The Pipeline class constructs a MultiDiGraph containing a collection of Activities. Each Activity is \
        arranged based on its dependencies and provide instructions for executing Tasks. \


        Args:
            steps (List[Type[Activity]]): List of Activities to add the DAG. Also excepts a tuple of positional args
                Ex: (task:Task, kwargs:dict, depends_on:List, skip_validation:bool, \
                    concurrency:int, retry:int, timeout:int, name:str, desc:str)
            type (Literal[basic; async], optional): Type of pipeline to construct. Defaults to 'basic'.
            gc_enabled (bool, optional): Runs garbage collection after every execution of a Task. Defaults to True.
        """
        Pipeline.pipeline_id += 1
        self.pid = Pipeline.pipeline_id
        self.steps = [create_activity(step) for step in steps]
        self.output = None
        self.queue = QueueFactory().get_queue(type=type)
        self.gc_enabled = gc_enabled
        self.gc = GarbageCollector()
        DAG.__init__(self)

    def _merge_dags(self, pipeline: "Pipeline") -> None:
        """Allow a Pipeline object to receive an other Pipeline object \
        by merging two Graphs together and preserving attributes.
        """
        pipeline.compose(self)
        G = pipeline.dag
        self.dag = self.merge(G, self.dag)
        self.repair_attributes(G, self.dag, 'activities')

    def _get_task_data(self, node_id: int, input_ref: List[str]) -> Tuple[Any]:
        """Looks up data required to run a task using the list of
        related UUIDs stored in the activity's related attribute.

        Args:
            node_id (int): node associated with the task
            input_ref (List[str]): list of uuids to lookup

        Returns:
            Tuple[Any]: data required for task
        """
        inputs = []
        for id in input_ref:
            node = self.get_all_nodes()[node_id]
            activity = node['activities'].get(id, None)
            if activity is not None:
                data = activity.result
                if data is not None:
                    inputs.append(data)
        return tuple(inputs)

    def _all_children_complete(self, n):
        """Check if all successor nodes have \
        run to completion

        Args:
            n (node): Node id in the graph
        Returns:
            bool: Returns True if all successor tasks have been \
            fully processed
        """
        # Get all the successor nodes
        successors = self.get_successors(n)
        # initialize a status list
        statuses = []
        # determine the status of successor tasks and append them to the list
        for succ in successors:
            activities = self.dag.nodes[succ]['activities']
            for v in activities.values():
                if v.status == 'Completed':
                    statuses.append(True)
                    continue
                else:
                    statuses.append(False)
        # if any statuses are False return false
        if False in statuses:
            return False
        return True

    def _proc_pipeline_dep(self, idx, activity, dep):
        """Process Dependencies that contain another Pipeline
        """
        # gets the last step from the pipeline dependency
        dep_task = dep.steps[-1].task

        # Validate task is compatible with the dependency
        if not activity.skip_validation:
            activity.task.validate(dep_task)

        # Update the activity with uuids of related runs
        if dep.dag.nodes[dep_task.tid].get('activities', None) is not None:
            for k in dep.dag.nodes[dep_task.tid]['activities'].keys():
                activity.related.append(k)
        else:
            raise DependencyError(f'{dep} was not found in {self.__name__}, check pipeline steps.')

        # Replace the Pipeline References with Task Reference
        activity.depends_on[idx] = dep_task

        return (activity, dep_task)

    def _proc_activity_dep(self, idx: int, activity: Activity, dep: str, input_pipe: "Pipeline"):
        """Process Dependencies that contain a reference to another activity"""
        # this is very ugly and needs to be refactored
        try:
            act = self.get_activity_by_name(name=dep)
            dag = self.dag
        except:
            act = input_pipe.get_activity_by_name(name=dep)
            dag = input_pipe.dag

        dep_task = act.task
        activity.depends_on[idx] = dep_task

        # Validate task is compatible with the dependency
        if not activity.skip_validation:
            activity.task.validate(dep_task)

        # Lookup dependent task from the current pipeline or the calling pipeline
        if dag.nodes[dep_task.tid].get('activities', None) is not None:
            for k in dag.nodes[dep_task.tid]['activities'].keys():
                activity.related.append(k)
        else:
            raise DependencyError(f'{dep_task} was not found in {self.__name__}, check pipeline steps.')

        return (activity, dep_task)

    def _proc_task_dep(self, activity, dep, input_pipe):
        """Processes Dependencies that contain a subclass of a Task.
        """

        # Validate task is compatible with the dependency
        if not activity.skip_validation:
            activity.task.validate(dep)

        # Lookup dependent task from the current pipeline
        pipe = self
        if dep.tid not in self.dag:
            pipe = input_pipe
        if pipe.dag.nodes[dep.tid].get('activities', None) is not None:
            for k in pipe.dag.nodes[dep.tid]['activities'].keys():
                for act in pipe.steps:
                    if k == act.id:
                        activity.related.append(k)
        else:
            raise DependencyError(f'{dep} was not found in {self.__name__}, check pipeline steps.')

        return (activity, dep)

    def _process_dep(self, idx: int, activity: Activity, dep: Any, input_pipe: "Pipeline") -> Tuple[Activity, Task]:
        """Basic Factory function for processing dependencies.

        Args:
            idx (int): index of the dependency in the dependency list
            activity (Activity): Activity to process
            dep (Any): Dependency object to process
            using: (DAG): the calling DAG

        Raises:
            TypeError: Complain if Object is not a valid dependency that
                can be processed.

        Returns:
            Tuple[Activity, Task]: Processed Activity & Task
        """
        if isinstance(dep, Pipeline):
            return self._proc_pipeline_dep(idx, activity, dep)
        elif isinstance(dep, str):
            return self._proc_activity_dep(idx, activity, dep, input_pipe)
        elif issubclass(type(dep), Task):
            return self._proc_task_dep(activity, dep, input_pipe)
        else:
            raise TypeError("Invalid Dependencies found in {self.__name__}: Activity {activity.__name__} ")

    def save(self, filename: str, protocol: str = None):
        """Serializes a DAG using cloudpickle

        Args:
            filename (str): name of file to write steam to.
            protocol (str, optional): protocol defaults to cloudpickle.DEFAULT_PROTOCOL  \
                which is an alias to pickle.HIGHEST_PROTOCOL.

        Usage:
        >>> filename = 'my_dag.pkl' # filename to use to serialize DAG
        >>> pipe = Pipeline(steps=my_steps) # create new pipeline instance with steps
        >>> pipe.compose() # compose a DAG from steps
        >>> pipe.save(filename) # save the DAG
        """

        with open(filename, 'wb') as f:
            import cne
            cpickle.register_pickle_by_value(module=cne)
            cpickle.dump(obj=self.dag, file=f, protocol=protocol)
        return self

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

    def garbage_collection(self, n):
        """Runs garbage collection after a task \
        has been fully processed.

        Args:
            n (node): Node Id in the graph
        """
        # Get all predecessors to n
        predecessors = self.get_predecessors(n)
        # for each predecessor
        for pred in predecessors:
            # check if its children have completed running
            if self._all_children_complete(pred):
                # delete the result attribute from the node activities
                for v in self.dag.nodes[pred]['activities'].values():
                    del v.result

    def print_plan(self):
        """Pretty Prints the DAG in Queue Processing Order"""

        from pprint import pformat

        nodes = self.get_all_nodes().copy()
        index_map = {v: i for i, v in enumerate(self.topological_sort())}
        return pformat(dict(sorted(nodes.items(), key=lambda pair: index_map[pair[0]])), compact=True, width=41)

    def get_activity_by_name(self, name: str) -> Activity:
        """Retrieves an Activity from the DAG using its name

        Args:
            name (str): The name of the Activity

        Raises:
            NotFoundError: Complains if the activity could not be found

        Returns:
            Activity: The activity that matches the name parameter.
        """
        for act_attrs in self.get_all_attributes(name='activities'):
            if act_attrs is not None:
                for act in act_attrs.values():
                    if act.name == name:
                        return act

        raise NotFoundError(f"{name} was not found in the DAG")

    def compose(self, input_pipe: "Pipeline" = None) -> None:
        """
        Compose the DAG from steps provided to the pipeline
        """
        # For each activity found in steps
        for activity in self.steps:
            # Process the activity with a special call if it is a Pipeline Instance
            if isinstance(activity.task, Pipeline):
                self._merge_dags(activity.task)
                continue

            # Process the dependencies
            if activity.depends_on is not None:
                # Sanity check what was provided as a dependency
                assert isinstance(activity.depends_on, List), TypeError(
                    f'Dependencies for Activity: {Activity.id} must be a list.')
                assert len(activity.depends_on) >= 1, ValueError(
                    f"No Dependencies Provided for Activity: {Activity.id}")

                for idx, dep_task in enumerate(activity.depends_on):
                    assert activity.task != dep_task, DependencyError(f'{activity.task} cannot be \
                        both the active task and dependency')

                    # Process the dependency
                    activity, dep_task = self._process_dep(idx, activity, dep_task, input_pipe)

                    # Add edge to DAG using activity id as an edge key
                    self.add_edge_to_dag(self.pid, dep_task.tid, activity.task.tid, activity.id)

            # Add Activity to node with related keys
            activity.related = list(dict.fromkeys(activity.related).keys())
            self.add_node_to_dag(activity)

        # Validates DAG was constructed properly
        self._validate_dag()

    def collect(self) -> None:
        """Enqueues all Tasks from the constructed DAG in topological sort order
        """
        # Compile steps into the DAG if not already compiled
        if self.is_empty():
            self.compose()

        nodes = self.get_all_nodes()
        # Get Topological sort of Node Ids
        for id in self.topological_sort():
            # Lookup each activity in a node
            n_attrs = nodes[id]
            # Enqueue Activities & update status
            for v in n_attrs['activities'].values():
                self.queue.enqueue(v)
                v.update_status('Queued')

    def run(self) -> Any:
        """Runs all Activities in the Pipeline.

        Returns:
            Any: Returns the output (if any exist) from the last processed Task.
        """

        # If Queue is empty, populate it
        if self.queue.is_empty():
            self.collect()

        # Execute the main loop to process Activities from the Queue
        while not self.queue.is_empty():
            # Get the activity from the queue to process
            activity = self.queue.dequeue()
            activity.update_status('Running')
            # Get inputs from dependencies
            if activity.depends_on:
                inputs = ()
                for dep_task in list(dict.fromkeys(activity.depends_on).keys()):
                    input_data = self._get_task_data(dep_task.tid, activity.related)
                    inputs = inputs + input_data
            else:
                inputs = tuple()
            # Run the task with instructions
            try:
                activity.run(inputs)
            except Exception as e:
                activity.update_status('Failed')
                raise ActivityFailedError(
                    "Activity {name} with id '{id}' failed with error: {err}".format(
                        name=activity.name,
                        id=activity.id,
                        err=e
                    )
                )
            # Activity is finished
            self.queue.is_done()
            # Update Status
            activity.update_status('Completed')
            # Run Garbage Collection
            if self.gc_enabled:
                self.garbage_collection(n=activity.task.tid)
                self.gc.collect()
