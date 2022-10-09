import unittest
from pyspark.sql import SparkSession
from cne.pipelines.tasks import Task, SparkTask
from typing import Tuple, Any, List


# udf
def bogus_func(x: int, y: int) -> int:
    x += y
    return x


class TestTasks(unittest.TestCase):

    def setUp(self) -> None:
        self.spark = SparkSession.builder \
            .master("local") \
            .appName('unit testing database api') \
            .getOrCreate()

    def test_task_creation(self):
        """Unit Testing Task Instantiation"""

        # Test Task is created when udf is provided
        tsk = Task(bogus_func, desc='Unit Testing Tasks')
        self.assertIsInstance(tsk, Task)

        # Test Task tid is unique
        tsk = Task(bogus_func, desc='Unit Testing Tasks 1')
        tsk1 = Task(bogus_func, desc='Unit Testing Tasks 2')
        tsk2 = Task(bogus_func, desc='Unit Testing Tasks 3')

        tsk_list = [tsk.tid, tsk1.tid, tsk2.tid]
        self.assertEqual(len(set(tsk_list)), 3)

        # Test description is created with task if provided
        self.assertIsNotNone(tsk.desc)

        # Test description is None if not provided
        tsk3 = Task(bogus_func)
        self.assertIsNone(tsk3.desc)

    def test_spark_task_creation(self):
        """Unit Testing Spark Task Creation"""
        # Test Spark Session is created if none is provided
        def bogus_func(x: int, y: int) -> int:
            x += y
            return x

        stsk = SparkTask(func=bogus_func)
        self.assertIsInstance(stsk, SparkTask)
        self.assertIsInstance(stsk._spark, SparkSession)

        # Test Spark session is used as singleton if provided
        stsk1 = SparkTask(func=bogus_func)
        self.assertEqual(stsk1._spark, stsk1._spark)

        # Test Spark Task Id is unique among all tasks
        tsk_list = [stsk1.tid, stsk.tid]
        self.assertEqual(len(set(tsk_list)), 2)

    def test_dunder_methods(self):
        """Unit Testing special dunders of a Task"""

        # create a task
        tsk = Task(bogus_func)

        # Test that dunder method is correct if corrective type is given
        self.assertEqual(tsk.__input__(), [int, int])
        self.assertEqual(tsk.__output__(), int)

        # Test that dunder outputs is a correct
        self.assertEqual(tsk.__output__(), int)

        # Test that dunder input is a list of strings
        self.assertEqual(tsk.__input__(), [int, int])

        # Test dunder if multiple outputs are given
        def multi_return(x: int, y: int) -> Tuple[int, int]:
            return x, y

        tsk_multi = Task(multi_return)
        output = tsk_multi.__output__()
        self.assertEqual(output, Tuple[int, int])

    def test_compatibility_checks(self):
        """Test """

        def bogus_func(x: int, y: int) -> int:
            x += y
            return x

        def bogus_func1(x: int, y: int) -> int:
            x += y
            return x

        def bogus_func2(x: str, y: str) -> Any:
            s = x + y
            return s

        def bogus_func3(x, y):
            s = x + y
            return s

        def bogus_func4(x: str, y: str) -> str:
            return x, y

        tsk = Task(bogus_func)
        tsk1 = Task(bogus_func1)
        tsk2 = Task(bogus_func2)
        tsk3 = Task(bogus_func3)
        tsk4 = Task(bogus_func4)

        # Test with happy path
        self.assertTrue(tsk1.validate(tsk))

        # Test with unhappy path
        with self.assertRaises(Exception):
            tsk2.validate(tsk1)

        # Test with ambiguous types
        with self.assertRaises(Exception):
            tsk3.validate(tsk2)

        # Test with no type given
        with self.assertRaises(Exception):
            tsk2.validate(tsk3)

        # Test with multiple types given for return
        self.assertTrue(tsk2.validate(tsk4))

    def test_task_runs(self):
        # Test Task runs successfully
        tsk = Task(bogus_func)
        outcome = tsk.run(x=1, y=2)
        self.assertEqual(outcome, 3)

        # Test Task throws an exception when provided a bad func
        def bad_func(x: int, y: str) -> List:
            x += y
            return x

        tsk_bad = Task(bad_func)
        with self.assertRaises(Exception):
            outcome = tsk_bad.run(x=1, y='2')


if __name__ == '__main__':
    unittest.main()
