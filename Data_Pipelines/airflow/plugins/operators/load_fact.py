from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    This class load fact tables based on sql queries passed in its params.
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query

    def execute(self, context):
        self.log.info("Connecting to Redshift using postgress hook")
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info(f"Run load query: {self.sql_query}")
        redshift.run(self.sql_query)

