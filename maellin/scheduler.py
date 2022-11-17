from apscheduler.schedulers.background import BackgroundScheduler


class DefaultScheduler(BackgroundScheduler):
    """Implements a Singleton Design Pattern for BackgroundScheduler
    
    The BackgroundScheduler runs a workflow in a seperate thread
    """
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DefaultScheduler, cls).__new__(cls)
            # Put any initialization here.
        return cls._instance
