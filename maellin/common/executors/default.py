import asyncio
from maellin.common.executors.base import BaseExecutor
from maellin.common.utils import get_task_result
from maellin.common.logger import LoggingMixin
from typing import TypeVar


Queue = TypeVar('Queue')


class DefaultWorker(LoggingMixin):
    """
    A Local based Worker that processes tasks sequentially.
    This worker does not support concurrency features
    """
    worker_id = 0

    def __init__(self, task_queue: Queue, result_queue: Queue):
        DefaultWorker.worker_id += 1
        self.task_queue = task_queue
        self.result_queue = result_queue
        self._log = self.logger

    def run(self):

        while not self.task_queue.empty():
            # Get the activity from the queue to process
            _task = self.task_queue.get()
            _task.update_status('Running')
            self._log.info('Running Task %s on Worker %s ' % (_task.name, self.worker_id))

            # Get inputs to use from dependencies
            if _task.depends_on:
                inputs = ()
                for dep_task in list(dict.fromkeys(_task.depends_on).keys()):
                    for completed_task in list(self.result_queue.queue):
                        if dep_task.tid == completed_task.tid:
                            input_data = get_task_result(completed_task)
                            inputs = inputs + input_data
            else:
                inputs = tuple()

            # Run the task with instructions
            _task.run(inputs)
            _task.update_status('Completed')

            # Put the results of the complete task in the result queue
            self.result_queue.put(_task)

            # Activity is finished running
            self.task_queue.task_done()


class DefaultExecutor(BaseExecutor):
    """Executes Tasks Sequentially using a single worker"""

    def __init__(self, task_queue, result_queue):
        super().__init__(task_queue, result_queue)

    def start(self):
        self._log.info('Starting Job %s' % self.job_id)
        self.worker = DefaultWorker(self.task_queue, self.result_queue)
        return self.worker.run()

    def end(self):
        """Removes the worker and results"""
        del self.worker


class AsyncIOWorker(LoggingMixin):
    """
    A Local based Worker that processes tasks sequentially.
    This worker does not support concurrency features
    """
    worker_id = 0

    def __init__(self, task_queue: Queue, result_queue: Queue):
        AsyncIOWorker.worker_id += 1
        self.task_queue = task_queue
        self.result_queue = result_queue
        self._log = self.logger

    async def run(self):

        while not self.task_queue.empty():
            # Get the activity from the queue to process
            _task = await self.task_queue.get()
            _task.update_status('Waiting')
            self._log.info('Waiting Task %s on Worker %s ' % (_task.name, self.worker_id))

            # Get inputs to use from dependencies
            if _task.depends_on:
                inputs = ()
                for dep_task in list(dict.fromkeys(_task.depends_on).keys()):
                    results_list = list(self.result_queue.queue)
                    for completed_task in results_list:
                        if dep_task.tid == completed_task.tid:
                            input_data = get_task_result(completed_task)
                            inputs = inputs + input_data
                        else:
                            await asyncio.sleep(0.1)
            else:
                inputs = tuple()

            # Run the task with instructions
            self._log.info('Running Task %s on Worker %s ' % (_task.name, self.worker_id))
            _task.update_status('Running')
            _task.run(inputs)
            _task.update_status('Completed')

            # Put the results of the complete task in the result queue
            self.result_queue.put_nowait(_task)

            # Activity is finished running
            self.task_queue.task_done()
            await asyncio.sleep(1)


class AsyncIOExecutor(BaseExecutor):
    """Executes Tasks Sequentially using a single worker"""

    def __init__(self, concurrency, task_queue, result_queue):
        super().__init__(task_queue, result_queue)
        self.concurrency = concurrency
        self.async_workers = []

    async def _start(self):
        self._log.info('Starting Job %s' % self.job_id)
        
        self.async_worker = AsyncIOWorker(
            self.task_queue,
            self.result_queue
        )
        for _ in range(self.concurrency):
            async_task_worker = asyncio.create_task(
                self.async_worker.run()
            )
            self.async_workers.append(async_task_worker)

        await self.task_queue.join()
        self.end()

    def start(self):
        asyncio.run(self._start())

    def end(self):
        """Removes the worker and results"""
        # Cancel our worker tasks when the queue is empty.
        for workers in self.async_workers:
            workers.cancel()
