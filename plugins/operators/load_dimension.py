from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift_default",
                 target_table=None,
                 sql_query=SqlQueries.user_table_insert,
                 insert_mode='truncate-insert',
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = 'users'
        self.sql_query=SqlQueries.user_table_insert
        self.insert_mode='truncate-insert'
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.insert_mode == "truncate-insert":
            self.log.info(f"Truncating {self.target_table}")
            redshift.run(f"TRUNCATE TABLE {self.target_table}")

        self.log.info(f"Inserting data into {self.target_table}")
        redshift.run(self.sql_query)
        self.log.info(f"Data loaded into {self.target_table} successfully")