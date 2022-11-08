# ============================ SAMPLE ============================ #
# This sample uses the DVD Rental Sample Database for PostgreSQL.  #
# This package does not install PostgreSQL but offers an API for   #
# working with PostgreSQL and executing SQL Queries. We do not     #
# make any depedencies to use a particular ORM at this time.       #
# This example demonstrats how a user's python code can be         #
# compiled into DAG containing Tasks to execute.                   #

from psycopg import Cursor
from pypika import PostgreSQLQuery, Schema, Column
from maellin.common.workflows import Pipeline
from maellin.common.tasks import Task
from maellin.common.clients.postgres import PostgresClient


# ============================ PARAMETERS ============================ #
DATABASE_CONFIG = '.config\\.postgres'
SECTION = 'postgresql'
DW = Schema('dssa')


# ============================ TABLE DEFINITIONS ============================ #
FACT_RENTAL = (
    Column('sk_customer', 'INT', False),
    Column('sk_date', 'INT', False),
    Column('sk_store', 'INT', False),
    Column('sk_film', 'INT', False),
    Column('sk_staff', 'INT', False),
    Column('count_rentals', 'INT', False)
)

DIM_CUSTOMER = (
    Column('sk_customer', 'INT', False),
    Column('name', 'VARCHAR(100)', False),
    Column('email', 'VARCHAR(100)', False)
)

DIM_STAFF = (
    Column('sk_staff', 'INT', False),
    Column('name', 'VARCHAR(100)', False),
    Column('email', 'VARCHAR(100)', False)
)

DIM_FILM = (
    Column('sk_film', 'INT', False),
    Column('rating_code', 'VARCHAR(20)', False),
    Column('film_duration', 'INT', False),
    Column('rental_duration', 'INT', False),
    Column('language', 'CHAR(20)', False),
    Column('release_year', 'INT', False),
    Column('title', 'VARCHAR(255)', False)
)

DIM_DATE = (
    Column('sk_date', 'TIMESTAMP', False),
    Column('quarter', 'INT', False),
    Column('year', 'INT', False),
    Column('month', 'INT', False),
    Column('day', 'INT', False),
)

DIM_STORE = (
    Column('sk_store', 'INT', False),
    Column('name', 'VARCHAR(100)', False),
    Column('address', 'VARCHAR(50)', False),
    Column('city', 'VARCHAR(50)', False),
    Column('state', 'VARCHAR(20)', False),
    Column('country', 'VARCHAR(50)', False)
)


# ============================ FUNCTIONS ============================ #
def create_cursor(path: str, section: str) -> Cursor:
    client = PostgresClient()
    conn = client.connect_from_config(path, section, autocommit=True)
    cursor = conn.cursor()
    return cursor


def create_schema(cursor: Cursor, schema_name: str) -> Cursor:
    q = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    cursor.execute(q)
    return cursor


def create_table(
        cursor: Cursor,
        table_name: str,
        definition: tuple,
        primary_key: str = None,
        foreign_keys: list = None,
        reference_tables: list = None) -> Cursor:

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

    ddl = ddl.get_sql()

    cursor.execute(ddl)
    return cursor


def tear_down(cursor: Cursor) -> None:
    cursor.execute("DROP SCHEMA DSSA CASCADE;")
    cursor.close()
    return


def main():
    # ============================ MAELLIN WORKFLOWS ============================ #
    setup_dw_workflow = Pipeline(
        steps=[
            Task(create_cursor,
                 kwargs={'path': DATABASE_CONFIG, 'section': SECTION},
                 depends_on=None,
                 name='create_cursor'),
            Task(create_schema,
                 kwargs={"schema_name": DW._name},
                 depends_on=['create_cursor'],
                 name='create_schema'),
            Task(create_table,
                 kwargs={'table_name': DW.dimCustomer, 'primary_key': 'sk_customer', 'definition': DIM_CUSTOMER},
                 depends_on=['create_schema'],
                 name='create_dim_customer'),
            Task(create_table,
                 kwargs={'table_name': DW.dimStore, 'primary_key': 'sk_store', 'definition': DIM_STORE},
                 depends_on=['create_schema'],
                 name='create_dim_store'),
            Task(create_table,
                 kwargs={'table_name': DW.dimFilm, 'primary_key': 'sk_film', 'definition': DIM_FILM},
                 depends_on=['create_schema'],
                 name='create_dim_film'),
            Task(create_table,
                 kwargs={'table_name': DW.dimStaff, 'primary_key': 'sk_staff', 'definition': DIM_STAFF},
                 depends_on=['create_schema'],
                 name='create_dim_staff'),
            Task(create_table,
                 kwargs={'table_name': DW.dimDate, 'primary_key': 'sk_date', 'definition': DIM_DATE},
                 depends_on=['create_schema'],
                 name='create_dim_date'),
            Task(create_table,
                 kwargs={
                     'table_name': DW.factRental, 'definition': FACT_RENTAL,
                     'foreign_keys': ['sk_customer', 'sk_store', 'sk_film', 'sk_staff', 'sk_date'],
                     'reference_tables': [DW.dimCustomer, DW.dimStore, DW.dimFilm, DW.dimStaff, DW.dimDate]},
                 depends_on=['create_schema'],
                 name='create_fact_rentals')
        ],
        type='default'
    )

    # ============================ COMPILATION ============================ #
    setup_dw_workflow.compose()

    # ============================ ENQUEUE ============================ #
    setup_dw_workflow.collect()

    # ============================ EXECUTION ============================ #
    # To run a Maellin Workflow locally using a single worker
    # This option is good for debugging before presisting the workflow
    # and submitting it to the scheduler.
    setup_dw_workflow.run()

    # To schedule Dag to run by submitting it to the scheduler
    # setup_dw_workflow.submit()


if __name__ == '__main__':
    main()
