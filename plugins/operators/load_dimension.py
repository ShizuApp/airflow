from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Insert data into a table using provided SELECT Sql statement

    Args:
    sql: complete select SQL query
    conn_id: name of Redshift's Airflow connection
    table: table name
    truncate: (bool) Empty table before insert Sql, 
    True by default
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 table="",
                 sql="",
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):
        rds_hook = PostgresHook(postgres_conn_id=self.conn_id)

        if self.truncate:
            self.log.info("Emptying table before running insert query")
            rds_hook.run(f"TRUNCATE {self.table}")

        rds_hook.run(
            f"""
            INSERT INTO {self.table}
            {self.sql}
            """
        )
