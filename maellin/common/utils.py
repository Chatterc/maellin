from uuid import uuid4, uuid5, NAMESPACE_OID
from typing import Tuple, Any


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