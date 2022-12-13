### Introduction
This walkthrough uses Maellin.io workflows with the DVD Rental Database to construct a star-schema using ETL.

__Extract, Transform & Load__ (ETL) is a process that extracts, transforms, and loads data from one or multiple sources to a __data warehouse__ or other unified data repository.

### What is our Use Case?

__Problem Statement__:  "We need to perfrom some analytics on data from a Movie rental store"
- The Data is stored in a highly normalized transactional database for online transactional processing. 
- We need to model the transactional data so that it is optimized for analytic queries using a __star-schema__.
- To implement this data model we need to a data processing pipeline that can __Extract, Transform, and Load__ data into a Data Warehouse.
- Linear Data Processing Pipelines are insufficient, __WE NEED A DAG__.


### About the DVD Rental Database
The DVD rental database represents the business processes of a DVD rental store as an OLTP PostgreSQL DB. 
> The DVD rental database has many objects including:
>15 tables
>1 trigger
>7 views
>8 functions
>1 domain
>13 sequences

15 tables in the DVD Rental database:

> * actor – stores actors data including first name and last name.
> * film – stores film data such as title, release year, length, rating, etc.
> * film_actor – stores the relationships between films and actors.
> * category – stores film’s categories data.
> * film_category- stores the relationships between films and categories.
> * store – contains the store data including manager staff and address.
> * inventory – stores inventory data.
> * rental – stores rental data.
> * payment – stores customer’s payments.
> * staff – stores staff data.
> * customer – stores customer data.
> * address – stores address data for staff and customers
> * city – stores city names.
> * country – stores country names.

__Entity Relationship Diagrams__ - An entity relationship diagram (ERD) shows the relationships of entity sets stored in a database. An entity in this context <u>is an object </u>, a component of data. 

By defining the entities, their attributes, and showing the relationships between them, an ER diagram illustrates the logical structure of databases.

The __DVD Rental Database ERD__ can be found in the `docs` folder of this repository as a PDF

---

### Objectives
The main objective of this lab is to implement an ETL process in python to create a __Star-Schema__ in a Data Warehouse that looks like the following:

![img](\../static/img/star-schema.jpg)

Put simply, we need to:
1. _extract_ data from a OLTP database called `dvdrental`
2.  _transform_ it by creating an aggregation of the count of rentals
3. _load_ the data into the `dw` Data Warehouse


__A walk-through of each table__

__Fact Table: FACT_RENTAL__
- `sk_customer` is the `customer_id` from customer table 
- `sk_date` is `rental_date` from the rental table
- `sk_store` the `store_id` from the store table
- `sk_film` is the `film_id` from the film table
- `sk_staff` is the `id` from the staff table
- `count_rentals` A count of the total rentals grouped by all other fields in the table
 
__Dimension Table: STAFF__
- `sk_staff` is the `staff_id` field from the staff table
- `name` a concatenation of `first_name` and `last_name` from the staff table
- `email` is the `email` field from the staff table


__Dimension Table: CUSTOMER__
- `sk_customer` is the `customer_id` from customer table
- `name` is the concatenation of `first_name` & `last_name` from the customer table
- `email` is the customer's email 

__Dimension Table: DATE__
- `sk_date` is unique `rental_date` converted into an integer so it can be used as a primary key 
- `quarter` is a column formatted from `rental_date` for quarter of the year
- `year` is a column formatted from `rental_date` for year
- `month` is a column formatted from `rental_date` for month of the year
- `day` is a column formatted from `rental_date` for day of the month

__Dimension Table: STORE__ 
- `sk_store` the `store_id` from the store table
- `name` (manager of the store) is the concatenation of `first_name` and `last_name` from the staff table
- `address` is the `address` field from the address table
- `city` is the `city` field from the city table
- `state` is the `district` field from the address table
- `country` is the `country field from the country table

__Dimension Table: FILM__
- `sk_film` is the `film_id` from the film table
- `rating_code` is the `rating` field from the film table
- `film_duration` is the `length` field from the film table
- `rental_duration` is the `rental_duration` from the film table
- `language` is the `name` field from the language table
- `release_year` is the `release_year` from the film table
- `title` is the `title` field from the film table
 
---

## __How Did I Organize My Project?:__
#### __Project Structure__

*   `.build` - This folder contains scripts related to the building the appplication (ex: Docker compose)
*   `.config` - This folder for configuration files (Example: `.json`, `.yaml`, `.toml`, `.ini`)
*   `.github` - folder for instructions related to contributing to the project & YAML files for github actions
*   `docs` - Documentation about Maellin.io Workflows Package.
*   `samples` - Place for getting started tutorials.
*   `maellin` - This is the source code folder containing all application code and modules.
*   static - A place for images
*   `tools` - A place for automation related scripts.
*   `LICENSE` - Open source license markdown
*   `README` - Markdown file describing the project
*   `SECURITY` - Markdown file for vunerability reporting
*   `requirements.txt` - list of python libraries to install with `pip`
*   `pyproject.toml` - toml file for building maellin whl for packaging the project. 

## How should my Project be used?
My project consist of two main features:
1. __Authoring__ - The ability to compose a DAG based workflow that is debugger friendly and strongly typed. 
<br>
2. __Scheduling & Execution__ - The ability to schedule authored DAGs as batch processing jobs.

## How Did I Develop My Python Modules?

__Tasks__ - This module implements the Task class that make up the individual nodes in my DAG. It is meant to wrap the ETL code that I will use when I author a workflow. I used a combination of Abstraction and Multiple Inheritance to develop unique features: 
*  A user should be able to validate that two or more tasks are compatible.
*  A Task should be able to execute a piece of ETL code

__Queue__ - This module implements an OOP creational design pattern called a `factory`. The Factory returns the compatible queue for the worker and executor to pull from. The users are abstracted from this queue.

__Scheduler__ - This python class implements a OOP designed pattern called a singleton. The singleton creates a new thread that runs jobs in the background of a single machine (deamon). It abstracts messy cron-like syntax with an easy configurable interface for batch jobs.

__Worker__ - A python class that implements a glorified for loop for getting tasks from the queue and processing them based on their instructions.

__Executor__ - The executor  is a python class that instantiates a set number of workers for processing the queue and runs the ETL job.

__DAG__ - This is a python class that encapsulates a networkx DiGraph. It implements several networkx functions for working with DAGs.

__Workflow__ - This python class is the main entry point for authoring an configuring how to execute a DAG. It has the ability to add Tasks to the DAG and serialize the DAG for processing by the Scheduler.


## How Should You Organize your `main.py`

The `main.py` is the entry point to our ETL program. It contains all the ETL code that needs to be executed to implement our star-schema. __We import our modules__ into the `main.py` to be used to prepare and execute our ETL code.

