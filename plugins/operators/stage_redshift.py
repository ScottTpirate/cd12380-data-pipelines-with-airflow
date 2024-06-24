from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)  # Enable templating for s3_key

    def __init__(
        self,
        redshift_conn_id: str,
        aws_credentials_id: str,
        s3_bucket: str,
        s3_key: str,
        target_table: str,
        copy_json_option: str = 'auto',
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.target_table = target_table
        self.copy_json_option = copy_json_option

    def execute(self, context):
        aws_hook = S3Hook(aws_conn_id=self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.target_table}")

        self.log.info("Listing S3 directories under the prefix")
        s3_client = aws_hook.get_conn()
        paginator = s3_client.get_paginator('list_objects_v2')
        directories = set()
        
        for page in paginator.paginate(Bucket=self.s3_bucket, Prefix=self.s3_key, Delimiter='/'):
            for prefix in page.get('CommonPrefixes', []):
                directories.add(prefix['Prefix'])

        if not directories:
            self.log.warning(f"No directories found under s3://{self.s3_bucket}/{self.s3_key}")
            return

        for directory in directories:
            self.log.info(f"Copying data from S3 to Redshift for directory {directory}")
            s3_path = f"s3://{self.s3_bucket}/{directory}"
            copy_query = f"""
                COPY {self.target_table}
                FROM '{s3_path}'
                ACCESS_KEY_ID '{credentials.access_key}'
                SECRET_ACCESS_KEY '{credentials.secret_key}'
                JSON '{self.copy_json_option}'
                REGION 'us-east-1';
            """
            redshift.run(copy_query)
            self.log.info(f"Data staged in Redshift table {self.target_table} from {s3_path}")





