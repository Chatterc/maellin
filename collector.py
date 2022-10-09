import gc
from typing import List, Optional


class GarbageCollector:

    def __init__(self):
        """Wrapper for the standard library gc module"""
        self.garbage_collect = gc

    def collect(self):
        """Collects garbage"""
        return self.garbage_collect.collect()

    def get_objects(self, generation: Optional[int] = None):
        """Gets a list of objects tracked by the garbage collector"""
        return self.garbage_collect.get_objects(generation=generation)

    def get_stats(self) -> List:
        """Gets statistics on garbage"""
        return self.garbage_collect()

    def get_garbage(self) -> List:
        """"Lists objects which the collector found  \
            to be unreachable but could not be freed
        """
        return self.garbage_collect.garbage()

    def is_enabled(self) -> bool:
        """Returns True if gc is enabled"""
        return self.garbage_collect.isenabled()
