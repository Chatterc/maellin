
import logging

FORMAT = '%(asctime)s :: %(name)s :: %(levelname)s :: %(message)s'

class LoggingMixin(object):
    """
    Convenient Mixin to have a logger configured with the class name
    """
    @property
    def logger(self):
        logging.basicConfig(format=FORMAT)
        name = '.'.join([self.__class__.__module__, self.__class__.__name__])
        return logging.getLogger(name).setLevel(logging.INFO)
