# ![img](static/img/logos/logo_01.png)




---

## __About__
Maellin.io is a lightweight workflows package for developing, scheduling, and monitor data processing in pure python. It comes with a rich set of APIs for connecting to a myraid of data sources and orchestrating tasks using directed acyclic graphs (DAGs).

---

## __Philosophy__
Citizen Developers & Engineers are often asked to tackle use cases that require quick setup and ease of use to solve data problems quickly. The modern ETL stack is often inundated with packages that require massive amounts of setup, configuration and endless black holes of documentation before a solution can be developed. Too often, novice or intermediate users lack the knowledge and expertise to write their own reusable components for orchestrating data intensive pipelines or end up over engineering a solution using a larger framework. Maellin.io aims to provide a ETL framework for small to medium sized workflows, with a focus on helping citizen devs and engineers catch bugs before they reach production. Maellin.io exposes developers to a rich set of APIs and ETL concepts that will help them graduate steadily to more complex projects and tools as their use cases and expertise grow. 

---
## __Project Structure__
This project structure should be used to help contributors understand where to "find" things and where to "put" new things. 
*   `.build` - This folder contains scripts related to the building the appplication (ex: Docker compose)
*   `.config` - This folder for configuration files (Example: `.json`, `.yaml`, `.toml`, `.ini`)
*   `.github` - folder for instructions related to contributing to the project & YAML files for github actions
*   `docs` - Documentation about Maellin.io Workflows Package.
*   `samples` - Place for getting started tutorials.
*   `maellin` - This is the source code folder containing all application code and modules.
*   `tools` - A place for automation related scripts.
*   `LICENSE` - Open source license markdown
*   `README` - Markdown file describing the project
*   `SECURITY` - Markdown file for vunerability reporting
*   `requirements.txt` - list of python libraries to install with `pip`
*   `pyproject.toml` - toml file for building maellin whl for packaging the project. 

---

## __Getting Started__

#### Installation

```bash
pip install maellin
```

### __Basic Usage__
_**Note**: All tutorials and samples are based on the DVD Rental Sample Database for Postgresql_

Maellin.io has three primary user flows 
1. Authoring - The process of composing a DAG from various tasks.
2. Scheduling - The process of scheduling a DAG for execution based on a schedule.
3. Monitoring - The ability to observe past, current and upcoming jobs.

__Authoring__ provides a debugging friendly experience for users to compose a DAG of Tasks. The primary entry point for authoring a DAG is the `Pipeline` class using the `steps` argument

The following example highlights how to author a simple pipeline consisting of a single task. The task uses the Maellin PostgresClient to return a DBAPI compatible cursor object for Postgresql.
```python
from maellin.workflows import Pipeline
from maellin.tasks import Task
from maellin.clients.postgres import PostgresClient

from typing import TypeVar


DATABASE_CONFIG = '.config\.postgres'
SECTION = 'postgresql'
Cursor = TypeVar('Cursor')

# A user-defined python function that uses the Maellin PostgresClient
# to create a DBAPI compatiable cursor object
def create_cursor(path:str, section:str) -> Cursor:
    client = PostgresClient()
    conn = client.connect_from_config(path, section, autocommit=True)
    cursor = conn.cursor()
    return cursor

# The Pipeline class is the entry point for adding Tasks into the DAG. 
# Any parameters you need to provide can be passed as keyword arguments to the task
# all kwargs will be bind to the callable when the is executed. 
workflow = Pipeline(
    steps=[
        Task(
            func=create_cursor, 
            depends_on=None, 
            name='create_cursor',
            desc="Creates the Database Cursor",
            skip_validation=False,
            path=DATABASE_CONFIG, 
            section=SECTION, 
        )
    ]
)

# The built-in run() method will execute any tasks in the DAG Locally
#  in a single worker for debugging purposes.
workflow.run()
```
See the full authoring [sample](/samples/00_authoring_workflows.py) that uses Maellin to to create a star-schema here.



#### Using Tasks
#TODO

#### Using Pipelines
#TODO

#### Using Database Clients
#TODO

#### Choosing an Executor
#TODO 

#### Configuring Jobs for Single or Multiple Workers
#TODO

#### Using the Scheduler
#TODO

### __Advanced Usage__

#### Using the Maellin.io CLI
#TODO

#### Using Maellin.io with Celery
#TODO

#### Using Maellin.io with Redis
#TODO

#### Using Maellin.io Tracking Server
#TODO

#### Using Maellin.io with FastAPI
#TODO

---
## __How to Contribute__
#TODO
---

## __How to Run Tests__
#TODO
---

## __Join the Conversation__
#TODO

