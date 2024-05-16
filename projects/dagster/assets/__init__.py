from dagster import Definitions, load_assets_from_modules, ConfigurableResource
from trino.dbapi import connect
from . import assets
from contextlib import contextmanager


class TrinoConnection(ConfigurableResource):
    host: str
    port: int
    user: str
    password: str

    @contextmanager
    def get_connection(self):
        conn = connect(host=self.host, port=self.port,
                       user=self.user)
        yield conn
        conn.close()


all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        'trino': TrinoConnection(
            host='localhost',
            port=8060,
            user='trino',
            password=''
        )
    }
)
