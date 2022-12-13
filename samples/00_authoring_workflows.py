# ============================ SAMPLE ============================ #
# This sample uses the DVD Rental Sample Database for PostgreSQL.  #
# This package does not install PostgreSQL but offers an API for   #
# working with PostgreSQL and executing SQL Queries. We do not     #
# make any depedencies to use a particular ORM at this time.       #
# This example shows the maellin.io authoring abilities and how    #
# users can author DAG based workflows in pure python.             #

import pandas as pd
from psycopg import Cursor
from pypika import PostgreSQLQuery, Schema, Column
from maellin.workflows import Pipeline
from maellin.tasks import Task
from maellin.clients.postgres import PostgresClient
from maellin.plotting import plot_dag

# ============================ PARAMETERS ============================ #
DATABASE_CONFIG = '.config\\.postgres'
SECTION = 'postgresql'
DW = Schema('dw')
DVD = Schema('public')


# ============================ TABLE DEFINITIONS ============================ #
# This section is for providing table definitions for any DB objects that need
# to be created in the pipeline. We currently use Pypika, which implements a builder
# pattern for constructing sql queries in python but other frameworks like
# SQLAlchemy can also be used.

# The fact Table of our star schema
FACT_RENTAL = (
    Column('sk_customer', 'INT', False),
    Column('sk_date', 'INT', False),
    Column('sk_store', 'INT', False),
    Column('sk_film', 'INT', False),
    Column('sk_staff', 'INT', False),
    Column('count_rentals', 'INT', False)
)

# A dimension Table for customers
DIM_CUSTOMER = (
    Column('sk_customer', 'INT', False),
    Column('name', 'VARCHAR(100)', False),
    Column('email', 'VARCHAR(100)', False)
)

# A dimension Table for staff
DIM_STAFF = (
    Column('sk_staff', 'INT', False),
    Column('name', 'VARCHAR(100)', False),
    Column('email', 'VARCHAR(100)', False)
)

# A dimension table for films
DIM_FILM = (
    Column('sk_film', 'INT', False),
    Column('rating_code', 'VARCHAR(20)', False),
    Column('film_duration', 'INT', False),
    Column('rental_duration', 'INT', False),
    Column('language', 'CHAR(20)', False),
    Column('release_year', 'INT', False),
    Column('title', 'VARCHAR(255)', False)
)

# A dimension table for dates
DIM_DATE = (
    Column('sk_date', 'INT', False),
    Column('date', 'TIMESTAMP', False),
    Column('quarter', 'INT', False),
    Column('year', 'INT', False),
    Column('month', 'INT', False),
    Column('day', 'INT', False),
)

# A dimension table for stores
DIM_STORE = (
    Column('sk_store', 'INT', False),
    Column('name', 'VARCHAR(100)', False),
    Column('address', 'VARCHAR(50)', False),
    Column('city', 'VARCHAR(50)', False),
    Column('state', 'VARCHAR(20)', False),
    Column('country', 'VARCHAR(50)', False)
)


# ============================ FUNCTIONS ============================ #
# This section contains all the ETL code that needs to be executed to build a star schema
def create_cursor(path: str, section: str) -> Cursor:
    client = PostgresClient()
    conn = client.connect_from_config(path, section, autocommit=True)
    cursor = conn.cursor()  # returns a cursor to send commands & queries to the connection
    return cursor


def create_schema(cursor: Cursor, schema_name: str) -> Cursor:
    q = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"  # We can provide strings containing SQL to the cursor
    cursor.execute(q)
    return cursor


def create_table(
        cursor: Cursor,
        table_name: str,
        definition: tuple,
        primary_key: str = None,
        foreign_keys: list = None,
        reference_tables: list = None) -> None:

    # Or we can use the builder to construct queries in python
    ddl = PostgreSQLQuery \
        .create_table(table_name) \
        .if_not_exists() \
        .columns(*definition)

    if primary_key is not None:
        ddl = ddl.primary_key(primary_key)

    if foreign_keys is not None:
        for idx, key in enumerate(foreign_keys):
            ddl.foreign_key(
                columns=key,
                reference_table=reference_tables[idx],
                reference_columns=key
            )

    ddl = ddl.get_sql()  # creates the SQL query that can be provided to the cursor

    cursor.execute(ddl)
    return


def read_table(cursor: Cursor, table_name: str, columns: tuple) -> pd.DataFrame:
    query = PostgreSQLQuery \
        .from_(table_name) \
        .select(*columns) \
        .get_sql()
    res = cursor.execute(query)
    data = res.fetchall()
    col_names = []
    for names in res.description:
        col_names.append(names[0])
    df = pd.DataFrame(data, columns=col_names)
    return df


def build_dim_customer(cust_df: pd.DataFrame) -> pd.DataFrame:
    cust_df.rename(columns={'customer_id': 'sk_customer'}, inplace=True)
    cust_df['name'] = cust_df.first_name + " " + cust_df.last_name
    dim_customer = cust_df[['sk_customer', 'name', 'email']].copy()
    dim_customer.drop_duplicates(inplace=True)
    return dim_customer


def build_dim_staff(staff_df: pd.DataFrame) -> pd.DataFrame:
    staff_df.rename(columns={'staff_id': 'sk_staff'}, inplace=True)
    staff_df['name'] = staff_df.first_name + " " + staff_df.last_name
    dim_staff = staff_df[['sk_staff', 'name', 'email']].copy()
    dim_staff.drop_duplicates(inplace=True)
    return dim_staff


def build_dim_dates(dates_df: pd.DataFrame) -> pd.DataFrame:
    dates_df = dates_df.copy()
    dates_df['sk_date'] = dates_df.rental_date.dt.strftime("%Y%m%d").astype('int')
    dates_df['date'] = dates_df.rental_date.dt.date
    dates_df['quarter'] = dates_df.rental_date.dt.quarter
    dates_df['year'] = dates_df.rental_date.dt.year
    dates_df['month'] = dates_df.rental_date.dt.month
    dates_df['day'] = dates_df.rental_date.dt.day
    dim_dates = dates_df[['sk_date', 'date', 'quarter', 'year', 'month', 'day']].copy()
    dim_dates.drop_duplicates(inplace=True)
    return dim_dates


def build_dim_store(
        store_df: pd.DataFrame,
        staff_df: pd.DataFrame,
        address_df: pd.DataFrame,
        city_df: pd.DataFrame,
        country_df: pd.DataFrame) -> pd.DataFrame:

    staff_df.rename(columns={'manager_staff_id': 'staff_id'}, inplace=True)
    staff_df['name'] = staff_df.first_name + " " + staff_df.last_name
    staff_df = staff_df[['staff_id', 'name']].copy()

    country_df = country_df[['country_id', 'country']].copy()
    city_df = city_df[['city_id', 'city', 'country_id']].copy()
    city_df = city_df.merge(country_df, how='inner', on='country_id')

    address_df = address_df[['address_id', 'address', 'district', 'city_id']].copy()
    address_df = address_df.merge(city_df, how='inner', on='city_id')
    address_df.rename(columns={'district': 'state'}, inplace=True)

    store_df.rename(columns={'manager_staff_id': 'staff_id'}, inplace=True)
    store_df.rename(columns={'store_id': 'sk_store'}, inplace=True)
    store_df = store_df.merge(staff_df, how='inner', on='staff_id')
    store_df = store_df.merge(address_df, how='inner', on='address_id')
    store_df = store_df[['sk_store', 'name', 'address', 'city', 'state', 'country']].copy()
    return store_df


def build_dim_film(film_df: pd.DataFrame, lang_df: pd.DataFrame) -> pd.DataFrame:

    film_df.rename(
        columns={'film_id': 'sk_film', 'rating': 'rating_code', 'length': 'film_duration'},
        inplace=True
    )

    lang_df.rename(
        columns={'name': 'language'},
        inplace=True
    )

    film_df = film_df.merge(lang_df, how='inner', on='language_id')
    film_df = film_df[['sk_film', 'rating_code', 'film_duration',
                       'rental_duration', 'language', 'release_year', 'title']].copy()
    return film_df


def build_fact_rental(
        rental_df: pd.DataFrame,
        inventory_df: pd.DataFrame,
        date_df: pd.DataFrame,
        film_df: pd.DataFrame,
        staff_df: pd.DataFrame,
        store_df: pd.DataFrame) -> pd.DataFrame:

    rental_df.rename(columns={'customer_id': 'sk_customer', 'rental_date': 'date'}, inplace=True)
    rental_df['date'] = rental_df.date.dt.date
    rental_df = rental_df.merge(date_df, how='inner', on='date')
    rental_df = rental_df.merge(inventory_df, how='inner', on='inventory_id')
    rental_df = rental_df.merge(film_df, how='inner', left_on='film_id', right_on='sk_film')

    rental_df = rental_df.merge(staff_df, how='inner', left_on='staff_id', right_on='sk_staff')
    rental_df = rental_df.merge(store_df, how='inner', on='name')

    rental_df = rental_df.groupby(['sk_customer', 'sk_date', 'sk_store', 'sk_film', 'sk_staff']).agg(
        count_rentals=('rental_id', 'count')).reset_index()

    rental_df = rental_df[['sk_customer', 'sk_date', 'sk_store', 'sk_film', 'sk_staff', 'count_rentals']].copy()
    return rental_df


def sink_data(cursor: Cursor, df: pd.DataFrame, target: str) -> None:
    data = tuple(df.itertuples(index=False, name=None))
    query = PostgreSQLQuery \
        .into(target) \
        .insert(*data) \
        .get_sql()
    cursor.execute(query)
    return


def tear_down(cursor: Cursor) -> None:
    cursor.close()
    return


def main():
    # ============================ AUTHORING WORKFLOWS ============================ #
    # This section uses maellin's Pipeline class to author a DAG based workflow
    # from the python functions defined above. Note that merging multiple pipelines together
    # is natively supported. When a Task with a dependency is created, the Pipeline calls
    # the Task.validate() method on the dependency to check for compatibility before
    # it is added to the DAG. Compatibility checks rely on type hints for all provided
    # arguments and return statements. To skip validation simply set skip_validation=True
    # when creating the Task. 

    # Creates a DAG for all the DDL commands to execute to create schema and tables
    # Workflows can be in the main or body of a .py script. They can even be put in 
    # python modules and imported into another python program.
    setup_workflow = Pipeline(
        steps=[
            Task(
                create_cursor,
                depends_on=None,
                name='create_cursor',
                path=DATABASE_CONFIG,
                section=SECTION
            ),
            Task(
                create_schema,
                depends_on=['create_cursor'],
                name='create_schema',
                schema_name=DW._name
            ),
            Task(
                create_table,
                depends_on=['create_schema'],
                name='create_dim_customer',
                table_name=DW.customer,
                primary_key='sk_customer',
                definition=DIM_CUSTOMER
            ),
            Task(
                create_table,
                depends_on=['create_schema'],
                name='create_dim_store',
                table_name=DW.store,
                primary_key='sk_store',
                definition=DIM_STORE
            ),
            Task(
                create_table,
                depends_on=['create_schema'],
                name='create_dim_film',
                table_name=DW.film,
                primary_key='sk_film',
                definition=DIM_FILM
            ),
            Task(
                create_table,
                depends_on=['create_schema'],
                name='create_dim_staff',
                table_name=DW.staff,
                primary_key='sk_staff',
                definition=DIM_STAFF
            ),
            Task(
                create_table,
                depends_on=['create_schema'],
                name='create_dim_dates',
                table_name=DW.date,
                primary_key='sk_date',
                definition=DIM_DATE
            ),
            Task(
                create_table,
                depends_on=['create_schema'],
                name='create_fact_rentals',
                table_name=DW.factRental,
                definition=FACT_RENTAL,
                foreign_keys=['sk_customer', 'sk_store', 'sk_film', 'sk_staff', 'sk_date'],
                reference_tables=[DW.customer, DW.store, DW.film, DW.staff, DW.date]
            )
        ],
        type='default'
    )

    cust_workflow = Pipeline(
        steps=[
            Task(
                read_table,
                depends_on=['create_cursor'],
                name='extract_cust',
                table_name=DVD.customer,
                columns=('customer_id', 'first_name', 'last_name', 'email')
            ),
            Task(
                build_dim_customer,
                depends_on=['extract_cust'],
                name='transf_cust'
            ),
            Task(
                sink_data,
                depends_on=['create_cursor', 'transf_cust', 'create_dim_customer'],
                name='load_customer',
                target=DW.customer,
            )
        ]
    )

    staff_workflow = Pipeline(
        steps=[
            Task(
                read_table,
                depends_on=['create_cursor'],
                name='extract_staff',
                table_name=DVD.staff,
                columns=('staff_id', 'first_name', 'last_name', 'email'),
            ),
            Task(
                build_dim_staff,
                depends_on=['extract_staff'],
                name='transf_staff'
            ),
            Task(
                sink_data,
                depends_on=['create_cursor', 'transf_staff', 'create_dim_staff'],
                name='load_staff',
                target=DW.staff,
            )
        ]
    )

    dates_workflow = Pipeline(
        steps=[
            Task(
                read_table,
                depends_on=['create_cursor'],
                name='extract_dates',
                table_name=DVD.rental,
                columns=('rental_id', 'rental_date', 'inventory_id', 'staff_id', 'customer_id')
            ),
            Task(
                build_dim_dates,
                depends_on=['extract_dates'],
                name='transf_dates'
            ),
            Task(
                sink_data,
                depends_on=['create_cursor', 'transf_dates', 'create_dim_dates'],
                name='load_dates',
                target=DW.date
            ),
        ]
    )

    store_workflow = Pipeline(
        steps=[
            Task(
                read_table,
                depends_on=['create_cursor'],
                name='extract_store',
                table_name=DVD.store,
                columns=('store_id', 'manager_staff_id', 'address_id')
            ),
            Task(
                read_table,
                depends_on=['create_cursor'],
                name='extract_address',
                table_name=DVD.address,
                columns=('address_id', 'address', 'city_id', 'district'),
            ),
            Task(
                read_table,
                depends_on=['create_cursor'],
                name='extract_city',
                table_name=DVD.city,
                columns=('city_id', 'city', 'country_id'),
            ),
            Task(
                read_table,
                depends_on=['create_cursor'],
                name='extract_country',
                table_name=DVD.country,
                columns=('country_id', 'country')
            ),
            Task(
                build_dim_store,
                depends_on=['extract_store', 'extract_staff', 'extract_address', 'extract_city', 'extract_country'],
                name='transf_store'
            ),
            Task(
                sink_data,
                depends_on=['create_cursor', 'transf_store', 'create_dim_store'],
                name='load_store',
                target=DW.store
            ),
        ]
    )

    film_workflow = Pipeline(
        steps=[
            Task(
                read_table,
                depends_on=['create_cursor'],
                name='extract_film',
                table_name=DVD.film,
                columns=('film_id', 'rating', 'length', 'rental_duration', 'language_id', 'release_year', 'title')
            ),
            Task(
                read_table,
                depends_on=['create_cursor'],
                name='extract_language',
                table_name=DVD.language,
                columns=('language_id', 'name')
            ),
            Task(
                build_dim_film,
                depends_on=['extract_film', 'extract_language'],
                name='transf_film'
            ),
            Task(
                sink_data,
                depends_on=['create_cursor', 'transf_film', 'create_dim_film'],
                name='load_film',
                target=DW.film,
            )
        ]
    )

    fact_workflow = Pipeline(
        steps=[
            Task(
                read_table,
                depends_on=['create_cursor'],
                name='extract_inventory',
                table_name=DVD.inventory,
                columns=('inventory_id', 'film_id', 'store_id')
            ),
            Task(
                build_fact_rental,
                depends_on=[
                    'extract_dates',
                    'extract_inventory',
                    'transf_dates',
                    'transf_film',
                    'transf_staff',
                    'transf_store'],
                name='transf_fact_rental'),
            Task(
                sink_data,
                depends_on=[
                    'create_cursor',
                    'transf_fact_rental',
                    'create_fact_rentals'],
                name='load_fact_rental',
                target=DW.factRental
            )
        ]
    )

    teardown_workflow = Pipeline(
        steps=[
            Task(
                tear_down,
                name='tear_down',
                depends_on=[
                    'create_cursor',
                    'load_film', # comes from film_workflow,
                    'load_store', # comes from store_workflow,
                    'load_dates', # comes from dates_workflow,
                    'load_staff', # comes from staff_workflow,
                    'load_customer', # comes from cust_workflow,
                    'load_fact_rental', # come from fact_workflow
                ],
            )
        ]
    )

    # We merge all the above Pipelines into a single Pipeline containing all Tasks to be added to the DAG.
    workflow = Pipeline(
        steps=[
            setup_workflow,
            cust_workflow,
            staff_workflow,
            dates_workflow,
            store_workflow,
            film_workflow,
            fact_workflow,
            teardown_workflow
        ]
    )

    # ============================ COMPILATION ============================ #
    # This section composes the DAG from the provided Tasks
    workflow.compose()

    # ============================  PLOTTING   ============================ #
    # Maellin comes with some limited plots for visualizing DAGs with
    # matplotlib and networkx
    plot_dag(workflow.dag, savefig=True, path='sample_dag.png')

    # ============================   ENQUEUE   ============================ #
    # Puts each task in a queue sorted in topological order
    workflow.collect()

    # ============================   SAVING    ============================ #
    # Maellin uses CloudPickle for saving and loading DAGs to the scheduler
    workflow.dump(filename='.dags/dvd_rental_workflow.pkl')

    # ============================  EXECUTION  ============================ #
    # To run a Maellin Workflow locally using a single worker
    # This option is good for debugging before presisting the workflow
    # and submitting it to the scheduler.
    workflow.run()


if __name__ == '__main__':
    # this part is needed to execute the program
    main()
