from abc import ABC, abstractmethod
from asyncio import Queue as AsyncQueue
from queue import Queue
from typing import Any, Type

from cne.pipelines.tasks import Task


class IQueue(ABC):

    @abstractmethod
    def enqueue(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def dequeue(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_size(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def is_empty(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def is_full(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def is_done(self) -> None:
        raise NotImplementedError


class TaskQueue(IQueue):

    def __init__(self, maxsize: int = 0):
        """Concrete Class for a simple thread safe FIFO Queue"""
        self.queue = Queue(maxsize)

    def enqueue(self, task: Type[Task], block: bool = True, timeout: float = None) -> None:
        """Enqueues a Task"""
        self.queue.put(task, block=block, timeout=timeout)

    def dequeue(self, block: bool = True, timeout: float = None) -> Any:
        """Dequeues a Task"""
        return self.queue.get(block=block, timeout=timeout)

    def get_size(self) -> int:
        """Gets the approximate size of the queue"""
        return self.queue.qsize()

    def is_empty(self) -> bool:
        """Return True is the queue is empty, otherwise False"""
        return self.queue.empty()

    def is_full(self) -> bool:
        """Return True if the queue is full, otherwise False."""
        return self.queue.full()

    def is_done(self) -> None:
        """Indicates a formerly enqueue task is processed"""
        self.queue.task_done()


class AsyncTaskQueue(IQueue):

    def __init__(self, maxsize: int = 0):
        """Concrete Class for using asyncio FIFO Queue
        Note: This queue is not thread safe
        """
        self.queue = AsyncQueue(maxsize)

    def enqueue(self, task: Type[Task]) -> None:
        """Enqueues a Task"""
        self.queue.put(task)

    def dequeue(self) -> Any:
        """Dequeues a Task"""
        return self.queue.get()

    def get_size(self) -> int:
        """Gets the approximate size of the queue"""
        return self.queue.qsize()

    def is_empty(self) -> bool:
        """Return True is the queue is empty, otherwise False"""
        return self.queue.empty()

    def is_full(self) -> bool:
        """Return True if the queue is full, otherwise False."""
        return self.queue.full()

    def is_done(self) -> None:
        """Indicates a formerly enqueue task is processed"""
        self.queue.task_done()


class QueueFactory:
    """Factory class that returns a Queue"""

    def get_queue(self, type: str) -> Type[IQueue]:
        if type == 'basic':
            return TaskQueue()

        elif type == 'async':
            return AsyncTaskQueue()

        else:
            raise ValueError(type)
