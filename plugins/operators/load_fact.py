from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    This operator loads data from a staging to a fact table in Redshift
    """

    copy_sql = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_query = select_query

    def execute(self, context):
        # fetch the redshift hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # format facts sql template
        formatted_sql = LoadFactOperator.copy_sql.format(self.table, self.select_query)
        #print (f"formatted_sql {formatted_sql}")
        self.log.info(f"Loading Fact table {self.table} ..")
        redshift.run(formatted_sql)
