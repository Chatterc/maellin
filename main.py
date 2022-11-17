import os
import time
from maellin.common.workflows import Pipeline
from maellin.common.scheduler import DefaultScheduler

NAME = 'myfinalproject'
sched = DefaultScheduler()
sched.start()

def create_jobs(path_to_dags:str):
    """Adds All Stored DAGs to The Scheduler"""
    
    # List all pickled dags using scandir()
    with os.scandir(path_to_dags) as entries:
        for entry in entries:
            name = os.path.splitext(entry)[0]
            if not entry.is_dir():
                
                # Get a list of all jobs tracked by the scheduler
                jobs = sched.get_jobs()
                
                if len(jobs) < 1:
                    workflow = Pipeline().load(filename=entry)
                    workflow.submit(name=name)
                    continue
                    
                # if the job matches our workflow then move along
                for job in jobs:
                    if job.name == name:
                        continue
                    else:
                        workflow = Pipeline().load(filename=entry)
                        workflow.submit(name=name, max_instances=1)

      
def main():
    # Poll Scheduler for scheduled dag runs
    while True:
        create_jobs('.dags')
        sched.print_jobs()
        time.sleep(5)
        
if __name__ == '__main__':
    main()