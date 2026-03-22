from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 dq_checks=None,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks or []

    def execute(self, context):
        if len(self.dq_checks) == 0:
            raise ValueError('No data quality checks were provided')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for check in self.dq_checks:
            check_sql = check.get('check_sql')
            expected_result = check.get('expected_result')

            if check_sql is None:
                raise ValueError('Each data quality check must define check_sql')

            records = redshift.get_records(check_sql)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError('Data quality check returned no results: {}'.format(check_sql))

            actual_result = records[0][0]
            if actual_result != expected_result:
                raise ValueError(
                    'Data quality check failed. SQL: {sql} | expected: {expected} | actual: {actual}'.format(
                        sql=check_sql,
                        expected=expected_result,
                        actual=actual_result
                    )
                )

            self.log.info('Data quality check passed. SQL: %s | result: %s', check_sql, actual_result)