from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"
    copy_statement = """
        COPY {table} from '{s3_bucket}/{s3_key}'
        ACCESS_KEY_ID '{access_key_id}'
        SECRET_ACCESS_KEY '{secret_access_key}'
        REGION '{s3_region}'
        COMPUPDATE OFF
        JSON '{s3_json_path}'
        TIMEFORMAT as 'epochmillisecs'
        BLANKSASNULL
        TRIMBLANKS
        TRUNCATECOLUMNS
        """

    @apply_defaults
    def __init__(self, table: str, s3_bucket: str, s3_key: str, s3_region: str, s3_json_path: str, *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.target_table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_region = s3_region
        self.s3_json_path = s3_json_path

    def execute(self, context):
        # Create S3Hook to get credentials
        aws_hook = AwsHook("aws_default")
        credentials = aws_hook.get_credentials()

        self.log.info("Connecting to Redshift cluster")
        redshift = PostgresHook()
        query = StageToRedshiftOperator.copy_statement.format(
            target_table=self.target_table,
            s3_bucket=self.s3_bucket,
            s3_key=self.s3_key,
            s3_region=self.s3_region,
            s3_json_path=self.s3_json_path,
            access_key_id=credentials.access_key,
            secret_access_key=credentials.secret_key,
        )
        redshift.run(query)

        self.log.info("Copying of data from S3 to Redshift is completed")
