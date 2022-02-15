from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    This operator stages data from S3 to Redshift
    """

    template_fields = ("s3_key",)
  
    copy_sql = """
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region '{}'
        json '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 #aws_credentials_id="",
                 aws_access_key_id="",
                 aws_secret_key_id="",
                 table="",                 
                 s3_region="",
                 s3_bucket="",
                 s3_key="",
                 s3_json_path="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        #self.aws_credentials_id = aws_credentials_id
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_key_id = aws_secret_key_id
        self.table = table
        self.s3_region = s3_region      
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_json_path = s3_json_path
        
    def execute(self, context):
        #aws_hook = AwsHook(self.aws_credentials_id)
        #credentials = aws_hook.get_credentials()
        aws_access_key = Variable.get(self.aws_access_key_id)
        aws_secret_key = Variable.get( self.aws_secret_key_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_access_key,
            aws_secret_key,
            self.s3_region,
            self.s3_json_path,
        )
        
        #print (f"formatted_sql {formatted_sql}")
        self.log.info(f"Copying data from S3 {s3_path} to Redshift table {self.table}")
        redshift.run(formatted_sql)
