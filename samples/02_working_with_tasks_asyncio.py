import time
import asyncio

from maellin.tasks import Task
from maellin.queues import QueueFactory


async def foo():
    print('I am foo and I am being worked on...')
    await asyncio.sleep(0.1)


async def bar():
    print('I am bar and I am being worked on...')
    await asyncio.sleep(0.1)


async def async_worker(queue):

    while not queue.empty():
        start_time = time.time()
        _tsk = await queue.get()
        print(f'Worker pulled \'{_tsk.func.__name__}\' from the Queue...')

        print('Running Task...')
        await _tsk.run()

        queue.task_done()
        print('Task Completed in %2f seconds' % (time.time() - start_time))


async def main():
    start_time = time.time()
    # Instantiate some Tasks for use to enqueue
    tsk_1 = Task(foo)
    tsk_2 = Task(bar)

    # Instantiate a queue using our Queue Factory
    q = QueueFactory.factory('asyncio')

    for i in range(100):
        q.put_nowait(tsk_1)
        q.put_nowait(tsk_2)

    # q.put_nowait(tsk_1)
    # q.put_nowait(tsk_2)

    # create an asyncio task that creates two workers to process the queue concurrently.
    async_tasks = []
    for i in range(20):
        task = asyncio.create_task(async_worker(q))
        async_tasks.append(task)

    await q.join()

    # Cancel our worker tasks when the queue is empty.
    for task in async_tasks:
        task.cancel()
    print('Asynchronous Pipeline Processed in %2f seconds' % (time.time() - start_time))

if __name__ == '__main__':
    asyncio.run(main())
