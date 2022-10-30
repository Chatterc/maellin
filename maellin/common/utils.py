from uuid import uuid4, uuid5, NAMESPACE_OID


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


