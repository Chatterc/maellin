from abc import ABCMeta, abstractclassmethod


class AbstractBaseClient(metaclass=ABCMeta):
    """Abstract Base Class for Maellin Database Clients
    """
    
    @abstractclassmethod
    def connect_from_config(self):
        NotImplementedError("Cannot be instantiated, abstract method must be subclassed")
        
    @abstractclassmethod
    def connect(self):
        NotImplementedError("Cannot be instantiated, abstract method must be subclassed")