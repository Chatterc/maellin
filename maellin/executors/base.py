#   Copyright (C) 2022  Carl Chatterton. All Rights Reserved.
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <https://www.gnu.org/licenses/>.

from abc import abstractclassmethod, ABCMeta
from maellin.utils import generate_uuid
from maellin.logger import LoggingMixin
from typing import TypeVar

Queue = TypeVar('Queue')


class AbstractBaseExecutor(metaclass=ABCMeta):
    """Abstract Base Class for Maellin Executors"""

    @abstractclassmethod
    def start(self):
        """Executors may need to get things started."""
        return NotImplementedError('Abstract Method that needs to be implemented by the subclass')

    @abstractclassmethod
    def shutdown(self):
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

    def shutdown(self):
        """Stops Execution of Tasks"""
        return
