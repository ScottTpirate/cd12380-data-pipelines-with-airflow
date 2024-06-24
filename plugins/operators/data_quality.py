from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift_default",
                 test_queries=None,
                 expected_results=None,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_queries = test_queries
        self.expected_results = expected_results

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.test_queries or not self.expected_results:
            raise ValueError("test_queries and expected_results must be provided and cannot be empty")

        if len(self.test_queries) != len(self.expected_results):
            raise ValueError("Each test query must have a corresponding expected result")

        for query, expected in zip(self.test_queries, self.expected_results):
            records = redshift_hook.get_records(query)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. No results returned for query: {query}")

            actual_result = records[0][0]
            if actual_result != expected:
                raise ValueError(f"Data quality check failed for query: {query}. Expected {expected}, but got {actual_result}")

            self.log.info(f"Data quality on query {query} check passed with expected result {expected}")

