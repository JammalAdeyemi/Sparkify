from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)

    copy_sql = """
        COPY {table}
        FROM 's3://{s3_bucket}/{s3_key}'
        ACCESS_KEY_ID '{aws_access_key_id}'
        SECRET_ACCESS_KEY '{aws_secret_access_key}'
        JSON '{json_path}'
        REGION '{region}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 json_path='auto',
                 region='us-east-1',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region

    def execute(self, context):
        aws_hook = AwsBaseHook(aws_conn_id=self.aws_credentials_id, client_type='s3')
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Clearing data from destination table %s', self.table)
        redshift.run(f'TRUNCATE TABLE {self.table}')

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            s3_bucket=self.s3_bucket,
            s3_key=self.s3_key,
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            json_path=self.json_path,
            region=self.region
        )

        self.log.info('Copying data from S3 to Redshift table %s', self.table)
        redshift.run(formatted_sql)