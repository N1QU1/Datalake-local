from dagster import (Definitions, load_assets_from_modules, ConfigurableResource, define_asset_job, AssetSelection,
                     ScheduleDefinition)
import psycopg2
from . import assets
from contextlib import contextmanager


class TrinoConnection(ConfigurableResource):
    host: str
    port: int
    user: str
    password: str
    dbname:str

    @contextmanager
    def get_connection(self):
        conn = psycopg2.connect(dbname=self.dbname,host=self.host, port=self.port,
                       user=self.user, password=self.password)
        cursor =conn.cursor()
        cursor.execute("select version();")

        yield conn
        conn.close()


all_assets = load_assets_from_modules([assets])
"""'trino': TrinoConnection(
           host='host.docker.internal',
           port=8060,
           user='trino',
           password=''
       ),"""
defs = Definitions(
    assets=all_assets,
    resources={
        'postgres':TrinoConnection(
            host='host.docker.internal',
            port=45432,
            user='ngods',
            password='ngods',
            dbname='ngods',
        )
    },
    jobs=[
        define_asset_job(
            name='Insert_excel_tables',
            selection=AssetSelection.groups('Data_Integration_excel'),
        ),
        define_asset_job(
            name="Process_csv_data",
            selection=AssetSelection.groups('Data_Integration_csv'),
        )
    ],
    schedules=[
        ScheduleDefinition(
            name='Periodic_insert',
            job_name='Insert_excel_tables',
            cron_schedule='*/30 * * * *',
        )
    ]
)
