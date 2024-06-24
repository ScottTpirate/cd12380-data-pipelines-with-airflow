from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift_default",
                 sql_query=None,
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = SqlQueries.songplay_table_insert
        


    def execute(self, context):
        if self.sql_query is None:
            raise ValueError("sql_query is required for LoadFactOperator")

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Loading data into fact table using query: {self.sql_query}")
        redshift_hook.run(self.sql_query)
        self.log.info("Data successfully loaded into the fact table.")
