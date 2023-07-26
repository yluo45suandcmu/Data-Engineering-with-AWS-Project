from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_cases=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_cases = test_cases

    def execute(self, context):
        self.log.info('DataQualityOperator implementing')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.test_cases is None:
            self.log.info('DataQualityOperator has no test cases')
            return
        
        error_count = 0
        failed_tests = []
        
        for test in self.test_cases:
            sql_query = test.get('check_sql')
            expected_result = test.get('expected_result')
            
            records = redshift.get_records(sql_query)
            
            if not records or not len(records[0]) or not records[0][0]:
                failed_tests.append(sql_query)
                error_count += 1
                self.log.error(f"Data quality check failed for query {sql_query}")
                continue
            
            if records[0][0] != expected_result:
                error_count += 1
                failed_tests.append(sql_query)

        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failed_tests)
            raise ValueError('Data quality checks failed')
        else:
            self.log.info('DataQualityOperator completed, all tests passed')
