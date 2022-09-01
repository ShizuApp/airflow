from airflow.hooks.postgres_hook import PostgresHook # deprecated
#from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook as AwsBaseHook # deprecated
#from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Copy data from S3 to Redshift using Airflow connections

    ### Args:
    conn_id: name of Redshift's Airflow connection
    aws_credentials: name of AWS credentials in the Airflow connection
    table: table name
    s3_bucket: S3 bucket name
    s3_key: S3 path inside the bucket
    json_path: (str) json_path or auto
    starting_date: start date to load timestamped files

    ### Example:
    ```
    stage_table_to_redshift = StageToRedshiftOperator(
        task_id='stage_table',
        dag=dag,
        conn_id="redshift",
        aws_credentials="aws_credentials",
        table="public.staging_songs",
        s3_bucket="udacity-dend",
        s3_key="song_data",
        json_path="auto"
    )
    ```
    """

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                conn_id="",
                aws_credentials="",
                table="",
                s3_bucket="",
                s3_key="",
                json_path="auto",
                starting_date=None,
                *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.starting_date = starting_date

    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials)
        credentials = aws_hook.get_credentials() # get credentials from Airflow
        rds_hook = PostgresHook(postgres_conn_id=self.conn_id)

        # s3 path
        path = f"s3://{self.s3_bucket}/{self.s3_key}"

        # load timestamped files if starting_date is provided
        if self.starting_date != None:
            path += f"/{self.starting_date.year}/{self.starting_date.month}/{context['ds']}-events.json"

        self.log.info(f"Copying data from {path} to Redshift")

        sql = f"""
            COPY {self.table}
            FROM '{path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            FORMAT AS JSON '{self.json_path}'
        """
        rds_hook.run(sql)
