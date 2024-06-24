from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift_default",
                 target_table=None,
                 sql_query=None,
                 insert_mode="truncate-insert",  # Default to truncate-insert mode
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.sql_query = sql_query
        self.insert_mode = insert_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.insert_mode == "truncate-insert":
            self.log.info(f"Truncating {self.target_table}")
            redshift.run(f"TRUNCATE TABLE {self.target_table}")

        self.log.info(f"Inserting data into {self.target_table}")
        redshift.run(self.sql_query)
        self.log.info(f"Data loaded into {self.target_table} successfully")