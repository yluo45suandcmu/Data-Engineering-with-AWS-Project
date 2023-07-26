from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 insert_mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.insert_mode = insert_mode

    def execute(self, context):
        self.log.info('LoadDimensionOperator implementing')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.insert_mode == "truncate-insert":
            redshift.run("DELETE FROM {}".format(self.table))

        insert_sql = f"INSERT INTO {self.table} {self.sql_query}"
        redshift.run(insert_sql)

