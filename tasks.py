from typing import Any, Callable, Optional, List
from inspect import signature
from pyspark.sql import SparkSession
from cne.pipelines.utils import CompatibilityException, MissingTypeHintException
from cne.pipelines.utils import generate_uuid


class Task(object):

    task_id = 0

    def __init__(self, func: Callable, desc: Optional[str] = None):
        """Container class for client UDFs. The Task Container
        is used to validate and run UDFs as part of a data processing
        pipeline.

        Args:
            func (Callable): the user defined function
            desc (Optional[str], optional): description of udf. Defaults to None.
        """
        Task.task_id += 1
        self.tid = generate_uuid()  # Task.task_id
        self.func = func
        self.desc = desc

    def __input__(self):
        """Parses the arguments of a client's UDF into a list

        Returns:
            annotation_list: annotated list of UDF arguments
        """
        annotation_list = [x.annotation for x in signature(self.func).parameters.values()]
        return annotation_list

    def __output__(self):
        """Parses the Return type from the client's UDF

        Returns:
            return_annotation : annotation for the return of a UDF
        """
        try:
            return_annotation = self.func.__annotations__['return']
            return return_annotation
        except:
            raise MissingTypeHintException(f"No type hint was provided for the {self.func.__name__}'s return")

    def __str__(self) -> str:
        from pprint import pprint
        s = dict()
        s['Task'] = self.__dict__.copy()
        s['Task']['input'] = self.__input__()
        s['Task']['output'] = self.__output__()
        return str(pprint(s))

    def __repr__(self) -> str:
        items = self.__dict__.copy()
        items['input'] = self.__input__()
        items['output'] = self.__output__()
        return '{}({})'.format(
            self.__class__.__name__,
            ', '.join('{}={!r}'.format(k, v) for k, v in items.items())
        )

    def validate(self, other: "Task") -> None:
        """Compatibility Checker that validates two tasks are compatible based on
        their respective inputs types (arguments) and output types (return).

        Args:
            other (Task): Upstream task to check for compatibility.

        Raises:
            CompatibilityException: If validation fails an exception is raised

        Returns:
            Boolean: Returns True if tasks are compatible
        """

        _val = any(other.__output__() is arg for arg in self.__input__())
        # if the output is Any, validation is not expected to work properly
        if other.__output__() is Any:
            error = f"Cannot check compatibility with previous task {other.func.__name__} when return type is 'Any'"
            raise CompatibilityException(error)

        if _val is not True:
            error = f"Validation Failed. Output of {other.func.__name__} " \
                + f"is incompatible with inputs from {self.func.__name__}"
            raise CompatibilityException(error)

        else:
            return True

    def run(self, *args, **kwargs):
        """Executes the UDF contained in the task

        Returns:
            result : Result of the UDF
        """
        return self.func(*args, **kwargs)


class SparkTask(Task):

    def __init__(self, func: Callable, desc: Optional[str] = None):
        """Spark Task for UDFs that require using the active
        SparkSession. The SparkTask treats the SparkSession
        as a singleton

        Args:
            func (Callable): client user defined function
            desc (str): description about the udf
        """
        Task.__init__(self, func=func, desc=desc)
        self._spark = SparkSession.builder.getOrCreate()

    def run(self, *args, **kwargs):
        """Executes the UDF contained in the task. accepts
        variable length and key word arguments needed for UDF

        Returns:
            result : Result of the UDF
        """
        result = self.func(self._spark, *args, **kwargs)
        return result


class ConditionalTask(Task):

    def __init__(self, func: Callable, func_true: Callable, func_false: Callable, desc: Optional[str] = None):
        Task.__init__(self, func=func, desc=desc)
        self.func_true = func_true
        self.func_false = func_false

    def __output__(self):
        try:
            return_annotation = (self.func_true.__annotations__['return'], self.func_false.__annotations__['return'])
        except:
            raise MissingTypeHintException(f"No type hint was provided for the {self.func.__name__}'s return")
        return return_annotation

    def validate(self, other: "Task") -> None:
        output = other.__output__()
        input = self.__input__()

        if isinstance(output, List):
            for out in output:
                if any(out is arg for arg in input):
                    _val = True
                else:
                    _val = False
                    break

        else:
            _val = any(output is arg for arg in input)

        if output is Any or "Any" in str(output):
            error = f"Cannot check compatibility with previous task with tid={other.tid} when return type is 'Any'"
            raise CompatibilityException(error)

        if _val is not True:
            error = f"Validation Failed. Output of task with tid={other.tid} " \
                + f"is incompatible with inputs from task with tid={self.tid}"
            raise CompatibilityException(error)

        else:
            return True

    def run(self, *args, **kwargs):

        condition = self.func(*args, **kwargs)

        if not isinstance(condition, bool):
            raise TypeError(f'{self.func.__name__} does not return True or False')

        if condition:
            return self.func_true(*args, **kwargs)
        if not condition:
            return self.func_false(*args, **kwargs)
