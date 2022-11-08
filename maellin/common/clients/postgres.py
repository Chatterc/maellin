from psycopg import connect, Connection
from psycopg.conninfo import make_conninfo
from configparser import ConfigParser
from maellin.common.clients.base import AbstractBaseClient


class PostgresClient(AbstractBaseClient):
    """Postgres client for working with postgres databases in python_
    """

    def __init__(self, host: str = None, port: int = None, user: str = None, password: str = None, dbname: str = None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname

    def connect_from_config(self, path: str, section: str, **kwargs) -> Connection:
        """Creates a psycopg3 Connection object from a configuration file

        Args:
            path (str): path to configuration file
            section (str): name of section in the configuration file

        Returns:
            Connection: a new connection instance
        """

        conn_dict = {}
        config_parser = ConfigParser()

        # Read the configuration file
        config_parser.read(path)
        if config_parser.has_section(section):
            config_params = config_parser.items(section)
            for k, v in config_params:
                conn_dict[k] = v

        conn = connect(
            conninfo=make_conninfo(**conn_dict),
            **kwargs
        )

        # checks if the connection is ok, it will throw an error if it is bad
        conn._check_connection_ok()

        return conn

    def connect(self, **kwargs) -> Connection:
        """Creates a psycopg3 Connection object from connection parameters
        passed as **kwargs. Alias for psycop3.connect()

        Returns:
            Connection: a new connection instance
        """
        conn = connect(
            conninfo=make_conninfo(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.dbname,
                **kwargs)
        )

        # checks if the connection is ok, it will throw an error if it is bad
        conn._check_connection_ok()

        return conn
