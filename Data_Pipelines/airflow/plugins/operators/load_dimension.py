from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    This class load dimensions tables based on sql queries passed in its params. It also truncates table before loading new data.
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 truncate=True,
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        self.log.info("Connecting to Redshift using postgress hook")
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.truncate:
            self.log.info(f"Truncating param is set to true so truncating the table: {self.table} before loading new one.")
            redshift.run(f"TRUNCATE TABLE {self.table}")
        self.log.info(f"Now loading dimension table: {self.table}")
        redshift.run(self.sql_query)
        self.log.info(f"{self.table} dimension table successfully loaded")