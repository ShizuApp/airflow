from airflow.hooks.postgres_hook import PostgresHook
#from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTableOperator(BaseOperator):
    """
    Drop table if exists and create a new table

    Args:
    sql: `CREATE TABLE` Sql query
    conn_id: name of Redshift's Airflow connection
    table: table name
    """
    
    ui_color = '#97FDFF'

    @apply_defaults
    def __init__(self,
                 sql="",
                 conn_id="",
                 table="",
                 *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        rds_hook = PostgresHook(postgres_conn_id=self.conn_id)

        self.log.info(f'Dropping {self.table} table in case it exists before execution')

        rds_hook.run(f"DROP TABLE IF EXISTS {self.table}")
        rds_hook.run(self.sql)
