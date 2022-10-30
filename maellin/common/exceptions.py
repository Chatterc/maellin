class CompatibilityException(Exception):
    pass


class MissingTypeHintException(Exception):
    pass


class OutputNotFoundError(Exception):
    pass


class CircularDependencyError(Exception):
    pass


class MissingDependencyError(Exception):
    pass


class DependencyError(Exception):
    pass


class ActivityFailedError(Exception):
    pass


class NotFoundError(Exception):
    pass
