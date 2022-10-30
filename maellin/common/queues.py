from asyncio import Queue as AsyncQueue
from queue import Queue as Queue
from multiprocessing import JoinableQueue
from typing import TypeVar


MaellinQueue = TypeVar("MaellinQueue", Queue, JoinableQueue, AsyncQueue)


class QueueFactory:
    """Factory class that returns a Maellin supported Queue"""
    @staticmethod
    def factory(type: str) -> MaellinQueue:
        if type == 'default':
            return Queue()
        elif type == 'multithread':
            return Queue()
        elif type == 'multiprocess':
            return JoinableQueue()
        elif type == 'asyncio':
            return AsyncQueue()
        else:
            raise ValueError(type)
