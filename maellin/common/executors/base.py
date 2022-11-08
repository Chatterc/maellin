from abc import abstractclassmethod, ABCMeta
from maellin.common.utils import generate_uuid
from maellin.common.logger import LoggingMixin
from typing import TypeVar

Queue = TypeVar('Queue')


class AbstractBaseExecutor(metaclass=ABCMeta):
    """Abstract Base Class for Maellin Executors"""

    @abstractclassmethod
    def start(self):
        """Executors may need to get things started."""
        return NotImplementedError('Abstract Method that needs to be implemented by the subclass')

    @abstractclassmethod
    def stop(self):
        """Executors may need to get things started."""
        return NotImplementedError('Abstract Method that needs to be implemented by the subclass')


class BaseExecutor(AbstractBaseExecutor, LoggingMixin):

    job_id = generate_uuid()

    def __init__(self, task_queue: Queue, result_queue: Queue):
        super().__init__()
        self.task_queue = task_queue
        self.result_queue = result_queue
        self._log = self.logger

    def start(self):
        """Starts workers for processing Tasks"""
        return

    def stop(self):
        """Stops Execution of Tasks"""
        return
