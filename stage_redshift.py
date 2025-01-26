from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.secrets.metastore import MetastoreBackend

class StageToRedshiftOperator(BaseOperator):
    """
    Custom Operator to copy data from S3 to Redshift.

    :param redshift_conn_id: Airflow connection ID for Redshift
    :param aws_credentials_id: Airflow connection ID for AWS credentials
    :param table: Redshift table to load data into
    :param s3_bucket: S3 bucket containing the data
    :param s3_key: S3 key or prefix of the data files
    :param json_path: Path to JSONPaths file for Redshift COPY command
    """
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        
    """

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        aws_credentials_id="",
        table="",
        s3_bucket="",
        s3_key="",
        json_path="auto",
        *args, **kwargs
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        postgres = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data from Redshift table {self.table}")
        postgres.run(f"DELETE FROM {self.table}")

        self.log.info(f"Copying data from s3 to Redshift table")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            self.json_path
        )

            
        
        self.log.info(f"Executing COPY command: {formatted_sql}")
        postgres.run(formatted_sql)
