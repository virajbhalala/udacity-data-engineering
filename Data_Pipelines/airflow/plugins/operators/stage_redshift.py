from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Runs a SQL command to load JSON files from S3 to Amazon Redshift.
    """
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        COMPUPDATE OFF
        FORMAT AS JSON '{}'
        TIMEFORMAT 'epochmillisecs'
        BLANKSASNULL EMPTYASNULL;
    """
    
    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table_name="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 json_path="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_path = json_path

    def execute(self, context):
        self.log.info("Connecting to AWS")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info("Connecting to Redshift using postgress hook")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data in destination table..")
        redshift.run("DELETE FROM {}".format(self.table_name))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table_name,
            "s3://{}/{}".format(self.s3_bucket, rendered_key),
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_path
        )
        self.log.info(f'running query: {formatted_sql}')
        redshift.run(formatted_sql)
        
        self.log.info(f'Successful copy of {self.table_name} from S3 to Redshift')