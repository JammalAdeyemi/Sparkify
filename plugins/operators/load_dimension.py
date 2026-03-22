from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql='',
                 truncate_insert=True,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate_insert = truncate_insert

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_insert:
            self.log.info('Truncating dimension table %s before load', self.table)
            redshift.run('TRUNCATE TABLE {}'.format(self.table))

        insert_sql = """
            INSERT INTO {table}
            {sql}
        """.format(table=self.table, sql=self.sql)

        self.log.info('Loading dimension table %s', self.table)
        redshift.run(insert_sql)