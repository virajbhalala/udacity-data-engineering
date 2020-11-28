from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Run data quality check based on sql queries passed in dq_checks params
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks
        
    def execute(self, context):
        self.log.info('Starting data quality checks following direction passed in the params')
        redshift = PostgresHook(self.redshift_conn_id)
        error_count = 0
        failed_tests = []
        # loop through list of data quality queries
        for dq in self.dq_checks:
            sql = dq.get('check_sql')
            correct_result = dq.get('result')
            this_result = redshift.get_records(sql)[0]
            # compare actual result vs obtained results of the query
            if correct_result != this_result[0]:
                # error encountered
                error_count += 1
                failed_tests.append(sql)
        # raise an error if any failed tests
        if error_count > 0:
            self.log.info('Following tests failed')
            for ft in  failed_tests:
                self.log.info(f'query: {ft}')
            raise ValueError('Data quality check failed')
        else:
            self.log.info('All tests have passed!!!!!')