# Maellin Workflows
---

## About Maellin Workflows
Maellin Workflows (or simply Maellin) is a lightweight python package to programmatically develop, schedule, and monitor data processing workflows using directed acyclic graphs containing tasks.

---

## Project Structure:
*   `.build` - This folder contains scripts related to the building the app (ex: Docker compose)
*   `.config` - This folder for any configuration files (Example: JSON, YAML, TOML files)
*   `.github` - folder for misc github markdowns related to contributing to the project
*   `.workflows` - folder containing YAML files for github actions
*   `docs` - Documentation about Maellin Workflows
*   `samples` - Place to provide "Hello World" examples
*   `Maellin` - Source code folder, this contains the application code
*   `tools` - A place for automation related scripts.
*   `LICENSE` - Open source license
*   `README` - Markdown file describing the project
*   `requirements.txt` - list of python libraries to install with `pip`
*   `setup.py` - setup script for packaging the project. 

### Maellin Source Code Structure:
*   `app` - Maellin Workflow web application
*   `assets` - Folder contains all media assets such as images
*   `clients` - Folder for various web clients
*   `common` - Folder for python modules shared across subprojects
*   `data` - Folder for static data the application code depends on or used in test cases
*   `tests` - Folder for unit, integration & turing tests
*   `utils` - Folder for helper functions
*   `__init__.py` - python file to store app version
*   `main.py` - Entry point to the application

