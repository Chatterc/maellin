from asyncio import Queue as AsyncQueue
from queue import Queue as ThreadSafeQueue
from multiprocessing import JoinableQueue
from typing import Union


class QueueFactory:
    """Factory class that returns a supported queue type """
    @staticmethod
    def factory(type: str = 'default') -> Union[TreadSafeQueue, AsyncQueue, JoinableQueue] :
        """Factory that returns a queue based on type

        Args:
            type (str): type of queue to use. Defaults to 
            FIFO thread-safe queue. Other accepted types are "multi-processing"
            "asyncio" or "multi-threading"

        Returns:
            Queue | JoinableQueue | AsyncQueue : Python Queue
        """
        if type == 'default':
            return TreadSafeQueue()
        elif type == 'multi-threading':
            return TreadSafeQueue()
        elif type == 'multi-processing':
            return JoinableQueue()
        elif type == 'asyncio':
            return AsyncQueue()
        else:
            raise ValueError(type)
