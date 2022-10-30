import unittest
from cne.pipelines.queues import TaskQueue


def foobar(string='foobar') -> str:
    s = f"You gave me {string}"
    return s


class TestQueue(unittest.TestCase):

    def setUp(self):
        self.queue = TaskQueue(maxsize=1)

    def test_enqueue(self) -> None:
        self.queue.enqueue(foobar)
        self.assertEqual(self.queue.queue.qsize(), 1)

    def test_dequeue(self) -> None:
        self.queue.queue.put(foobar)
        task = self.queue.dequeue()
        self.assertEqual(task, foobar)

    def test_get_size(self) -> None:
        self.assertEqual(self.queue.get_size(), 0)

    def test_is_empty(self) -> None:
        self.assertTrue(self.queue.is_empty(), True)

    def test_is_full(self) -> None:
        self.queue.queue.put(foobar)
        self.assertTrue(self.queue.is_full(), True)

    def test_is_done(self) -> None:
        self.queue.queue.put(foobar)
        self.queue.queue.get()
        self.queue.is_done()

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
