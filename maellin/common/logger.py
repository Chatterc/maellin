
import sys
import logging


FORMAT = '%(asctime)s :: %(name)s :: %(levelname)s :: %(message)s'


class LoggingMixin(object):
    """
    Convenient Mixin to have a logger configured with the class name
    """
    @property
    def logger(self, level: str = 'INFO', **kwargs):
        logging.basicConfig(stream=sys.stdout, level=level, format=FORMAT, **kwargs)
        name = '.'.join([self.__class__.__module__, self.__class__.__name__])
        return logging.getLogger(name)
