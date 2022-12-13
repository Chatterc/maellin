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

from typing import Any, Tuple, Callable
from functools import partial, update_wrapper
from uuid import NAMESPACE_OID, uuid4, uuid5


def generate_uuid(name: str = None) -> str:
    """generate a unique identifier

    Args:
        namespace (str): makes a UUID using a SHA-1 hash of a namespace UUID and a name

    Returns:
        str(UUID): unique id
    """
    if name:
        return str(uuid5(NAMESPACE_OID, name))
    return str(uuid4())


def get_task_result(task) -> Tuple[Any]:
    """Looks up data required to run a task using the list of
    related UUIDs stored in the activity's related attribute.

    Args:
        node_id (int): node associated with the task
        input_ref (List[str]): list of uuids to lookup

    Returns:
        Tuple[Any]: data required for task
    """
    inputs = []
    if task is not None:
        data = task.result
        if data is not None:
            inputs.append(data)
    return tuple(inputs)


def wrapped_partial(func, *args, **kwargs) -> Callable:
    """creates a partial object and preserved attributes of the original function

    Args:
        func (_type_): Any python callable

    Returns:
        partial: returns an updated partial object
    """
    partial_func = partial(func, *args, **kwargs)
    update_wrapper(partial_func, func)
    return partial_func
