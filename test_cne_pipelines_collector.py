import unittest
from cne.pipelines.collector import GarbageCollector


def create_cycle(i):
    x = {}
    x[i + 1] = x
    print(x)


class Foo:
    pass


class TestGarbageCollector(unittest.TestCase):

    def setUp(self):
        self.gc = GarbageCollector()
        self.gc.garbage_collect.disable()

    def test_list(self):
        llist = []
        llist.append(llist)
        self.gc.collect()
        del llist
        self.assertEqual(self.gc.collect(), 1)

    def test_dict(self):
        d = {}
        d[1] = d
        self.gc.collect()
        del d
        self.assertEqual(self.gc.collect(), 1)

    def test_tuple(self):
        # since tuples are immutable we close the loop with a list
        llist = []
        t = (llist,)
        llist.append(t)
        self.gc.collect()
        del t
        del llist
        self.assertEqual(self.gc.collect(), 2)

    def test_class(self):
        class A:
            pass
        A.a = A
        self.gc.collect()
        del A
        self.assertNotEqual(self.gc.collect(), 0)

    def test_newstyleclass(self):
        class A(object):
            pass
        self.gc.collect()
        del A
        self.assertNotEqual(self.gc.collect(), 0)

    def test_instance(self):
        class A:
            pass
        a = A()
        a.a = a
        self.gc.collect()
        del a
        self.assertNotEqual(self.gc.collect(), 0)

    def test_newinstance(self):
        class A(object):
            pass
        a = A()
        a.a = a
        self.gc.collect()
        del a
        self.assertNotEqual(self.gc.collect(), 0)

        class B(list):
            pass

        class C(B, A):
            pass
        a = C()
        a.a = a
        self.gc.collect()
        del a
        self.assertNotEqual(self.gc.collect(), 0)
        del B, C
        self.assertNotEqual(self.gc.collect(), 0)
        A.a = A()
        del A
        self.assertNotEqual(self.gc.collect(), 0)
        self.assertEqual(self.gc.collect(), 0)

    def test_method(self):
        # Tricky: self.__init__ is a bound method, it references the instance.
        class A:
            def __init__(self):
                self.init = self.__init__
        a = A()
        self.gc.collect()
        del a
        self.assertNotEqual(self.gc.collect(), 0)

    def test_function(self):
        # Tricky: f -> d -> f, code should call d.clear() after the exec to
        # break the cycle.
        d = {}
        exec("def f(): pass\n", d)
        self.gc.collect()
        del d
        self.assertEqual(self.gc.collect(), 2)
