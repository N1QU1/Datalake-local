from dagster import (Definitions, load_assets_from_modules, ConfigurableResource, define_asset_job, AssetSelection,
                     ScheduleDefinition)
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
            host='host.docker.internal',
            port=8060,
            user='trino',
            password=''
        )
    },
    jobs=[
        define_asset_job(
            name='Insert_excel_tables',
            selection=AssetSelection.groups('Data_Integration_excel'),
        ),
        define_asset_job(
            name="Insert_json_tables",
            selection=AssetSelection.groups('Data_Integration_json'),
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
        ),
        ScheduleDefinition(
            name='Json_insertion',
            job_name='Insert_json_tables',
            cron_schedule='*/2 * * * *',
        )
    ]
)
