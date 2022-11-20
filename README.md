# ![img](static/img/logos/logo_01.png)

---

## __About Maellin.io__
Maellin.io is a lightweight workflows package for developing, scheduling, and monitor data processing in pure python. It comes with a rich set of APIs for connecting to a myraid of data sources and orchestrating tasks using directed acyclic graphs (DAGs).

---

## __Maellin.io Philosophy__
Citizen Developers & Engineers are often asked to tackle use cases that require quick setup and ease of use to solve data problems quickly. The modern ETL stack is often inundated with packages that require massive amounts of setup, configuration and endless black holes of documentation before a solution can be developed. Too often, novice or intermediate users lack the knowledge and expertise to write their own reusable components for orchestrating data intesive tasks or end up over engineering a solution using a larger framework. Maellin.io aims to provide a ETL framework for small to medium sized workflows, with a focus on helping citizen devs and engineers catch bugs before they reach production. Maellin.io exposes developers to a rich set of APIs and ETL concepts that will help them graduate steadily to more complex projects and tools as their use cases and expertise grow. 

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

### __Basic Usage__
Maellin.io has two primary user flows 
1. Authoring 
2. Scheduling

__Authoring__ provides a debugging friendly experience for users to compose a DAG of Tasks. The primary entry point for authoring a DAG is the `Pipeline` class using the `steps` argument

The following example highlights how to author a pipeline with a single task using the Maellin PostgresClient to return a cursor object
```python
from maellin.workflows import Pipeline
from maellin.tasks import Task
from maellin.clients.postgres import PostgresClient


DATABASE_CONFIG = '.config\.postgres'
SECTION = 'postgresql'


# A user-defined python function that uses the Maellin PostgresClient to create a cursor object
def create_cursor(path:str, section:str) -> Cursor:
    client = PostgresClient()
    conn = client.connect_from_config(path, section, autocommit=True)
    cursor = conn.cursor()
    return cursor

# The Pipeline class is the entry point for adding Tasks into the DAG. Any keyword arguments required that you provided will bind to the callable when the is executed. 
workflow = Pipeline(
    steps=[
        Task(
            func=create_cursor, 
            kwargs={'path': DATABASE_CONFIG, 'section': SECTION}, 
            depends_on=None, 
            name='create_cursor'
        )
    ]
)

# The built-in run() method will execute any tasks in the DAG Locally in a single worker for debugging purposes.
workflow.run()
```
see a full [sample](/samples/00_authoring_workflows.py) of how to author workflows to create a star-schema here.


#### Installation

```bash
pip install maellin
```
#### Using Tasks
#### Using Workflows
#### Choosing an Executor
#### Using Single or Multiple Workers
#### Using the Scheduler

### __Advanced Usage__
#### Using the Maellin.io CLI

---
## __How to Contribute__

---

## __How to Run Tests__

---

## __Join the Conversation__
- __Slack Channel__
- __Discord__

