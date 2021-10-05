from typing import List

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self, table: str, columns: List[str], sql_statement: str, truncate_table: bool = True, *args, **kwargs
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.columns = columns
        self.sql = sql_statement
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.debug("Connecting to Redshift cluster")
        redshift = PostgresHook()

        if self.truncate:
            redshift.run(f"TRUNCATE {self.table}")

        self.log.info(f"Populating dimension table: {self.table}")
        redshift.run(f"INSERT INTO {self.table} ({','.join(self.columns)}) {self.sql_statement}")
