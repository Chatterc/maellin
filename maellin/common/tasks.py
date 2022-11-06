from typing import Any, Callable, TypeVar, List
from inspect import signature
from maellin.common.logger import LoggingMixin
from maellin.common.exceptions import CompatibilityException, MissingTypeHintException
from maellin.common.utils import generate_uuid
from abc import ABCMeta, abstractclassmethod


Task = TypeVar('Task')


class AbstractBaseTask(metaclass=ABCMeta):
    """Abstract Base Class of Task that cannot be instantiated and must be 
    implemented by the BaseTask class
    """
    @abstractclassmethod
    def validate(self):
        raise NotImplementedError('Abstract Method that needs to be implemented by the subclass')
    
    @abstractclassmethod
    def run(self):
        raise NotImplementedError('Abstract Method that needs to be implemented by the subclass')


class BaseTask(AbstractBaseTask, LoggingMixin):
    """Base Task provides implementation to validate method for callables before running them
    
    Args:
        func (Task): A Python callable (usually a function)
    """
    
    def __init__(self, func: Callable) -> None:
        super().__init__()
        self.tid = generate_uuid()
        self.func = func
        self._log = self.logger

    def __input__(self) -> List:
        """Gets the type annotations for all arguments in a python callable

        Returns:
            annotation_list: annotated list of acceptable types
        """
        annotation_list = [x.annotation for x in signature(self.func).parameters.values()]
        return annotation_list

    def __output__(self) -> Any:
        """Gets the return type annotation for a python callable

        Returns:
            return_annotation : type annotation for the return statement of func
        """
        try:
            return_annotation = self.func.__annotations__['return']
            return return_annotation
        except:
            raise MissingTypeHintException(f"No type hint was provided for {self.func.__name__}'s return")

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

    def validate(self, other: Task) -> bool:
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
            self._log.info('Validation Check Complete for %s' % self.func.__name__)
            return True

    def run(self, *args, **kwargs) -> Any:
        """Executes the python Callable

        Returns:
            Any: 
        """
        self._log.info("Task %s :: Running %s" % (self.tid, self.func.__name__))
        try:
            return self.func(*args, **kwargs)
        except Exception as error:
            self._log.exception(error, exc_info=True, stack_info=True)
            raise error