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

from asyncio import Queue as AsyncQueue
from queue import Queue as ThreadSafeQueue
from multiprocessing import JoinableQueue
from typing import Union


class QueueFactory:
    """Factory class that returns a supported queue type """
    @staticmethod
    def factory(type: str = 'default') -> Union[ThreadSafeQueue, AsyncQueue, JoinableQueue]:
        """Factory that returns a queue based on type

        Args:
            type (str): type of queue to use. Defaults to
            FIFO thread-safe queue. Other accepted types are "multi-processing"
            "asyncio" or "multi-threading"

        Returns:
            Queue | JoinableQueue | AsyncQueue : Python Queue
        """
        if type == 'default':
            return ThreadSafeQueue()
        elif type == 'multi-threading':
            return ThreadSafeQueue()
        elif type == 'multi-processing':
            return JoinableQueue()
        elif type == 'asyncio':
            return AsyncQueue()
        else:
            raise ValueError(type)
