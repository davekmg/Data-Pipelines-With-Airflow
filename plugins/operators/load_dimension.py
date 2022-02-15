from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    This operator loads data from a staging to a dimension table in Redshift
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
                 mode="append-only",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_query = select_query
        self.mode = mode

    def execute(self, context):
        # fetch the redshift hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # delete records from table if mode is set to delete-load
        if self.mode == "delete-load":
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

        # format dimension sql template
        formatted_sql = LoadDimensionOperator.copy_sql.format(self.table, self.select_query)
        #print (f"formatted_sql {formatted_sql}")
        self.log.info(f"Loading Dimension table {self.table} ..")
        redshift.run(formatted_sql)
