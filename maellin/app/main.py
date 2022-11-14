
import pathlib
import os
import uvicorn
from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler

from models.jobs import Job

app = FastAPI(
    title="Maellin.io Web Services",
    description=("Schedule and execute registered DAGs"),
    version="1.0.0"
)


def create_dirs(dags_dir:str = '.dags', job_dir:str = '.jobs'):
    # Create a dir for DAGs if it does not exist
    dags_dir = pathlib.Path(dags_dir)
    dags_dir.mkdir(parents=True, exist_ok=True)
    
    # Create dir for jobs if it does not exist
    job_dir = pathlib.Path(job_dir)
    job_dir.mkdir(parents=True, exists_ok=True)


def run_scheduled_dags(dags_dir:str = '.dag'):
    # Scan the DAG default directory for any presisted DAG files
    with os.scandir(dags_dir) as dag_entries:
        for dag in dag_entries:
            # If a DAG is found
            if not dag.is_dir():
                # Load it to a workflow
                print(dag)
                

@app.on_event('startup')
def start_scheduler():
    # Create required directories
    create_dirs()
    
    # instantiate the scheduler
    scheduler = BackgroundScheduler()
    
    # Add DAGs to the scheduler
    scheduler.add_job(
        func=run_scheduled_dags,
        trigger='interval',
        minutes=5,
        max_instances=1,
        executor='default',
        jobstore='default',
        replace_existing=True,
    )
    scheduler.start()


@app.get("/")
async def root():
    return {"message": "Hello DSSA Welcome to my Final Project"}


@app.post("/register", status_code=200)
async def register_dag(job: Job):
    dag = Pipeline.loads(job.dag)
    dag.dump(filename=f'.dags/{job.name}')
 

if __name__ == '__main__':
    uvicorn.run(app="main:app", host="127.0.0.1", port=80, reload=True, debug=False)