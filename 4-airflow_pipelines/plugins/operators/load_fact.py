from typing import List

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = "#F98866"

    @apply_defaults
    def __init__(
        self, table: str, columns: List[str], sql_statement: str, truncate_table: bool = True, *args, **kwargs
    ):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.columns = columns
        self.sql_statement = sql_statement
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info("Connecting to Redshift cluster")
        redshift = PostgresHook()

        if self.truncate_table:
            redshift.run(f"TRUNCATE TABLE IF EXISTS {self.table}")

        self.log.info(f"Populating table {self.table}")
        redshift.run(f"INSERT INTO {self.table} ({','.join(self.columns)}) {self.sql_statement}")
