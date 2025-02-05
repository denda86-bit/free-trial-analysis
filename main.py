import yaml
from classes import (Interface_s3_redshift)

if __name__ == "__main__":

    with open('config.yaml') as file:
        connect_dict = yaml.load(file)

    DBNAME = connect_dict["redshift"]["dbname"]
    PORT = connect_dict["redshift"]["port"]
    USER = connect_dict["redshift"]["user"]
    PASSWORD = connect_dict["redshift"]["password"]
    HOST = connect_dict["redshift"]["host"]
    SCHEMA = connect_dict["redshift"]["schema"]
    AWS_ACCESS_KEY_ID = connect_dict["s3_bucket"]["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY_ID = connect_dict["s3_bucket"]["aws_secret_access_key"]
    S3_TABLES_CSV = connect_dict["s3_tables"]
    AWS_BUCKET = connect_dict["s3_bucket"]["bucket"]
    interface_s3_redshift = Interface_s3_redshift(dbname=DBNAME, host=HOST, port=PORT, user=USER,
                                                  password=PASSWORD, schema=SCHEMA,
                                                  s3_tables_csv=S3_TABLES_CSV,
                                                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                                                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID,
                                                  aws_bucket=AWS_BUCKET)
    # TASKS EXECUTION :
    # 1. the redshift table in your personal schema
    # 2. code to used to load the S3 csv files into Redshift database
    interface_s3_redshift.load_db_from_s3_to_redshift()
    interface_s3_redshift.join_tables_redshift_db()
    interface_s3_redshift.conn.close()

    # ------------IMPORTANT----------
    # While executing the task I came across this error:
    # psycopg2.errors.InternalError_: Load into table 'purchases' failed.
    # Check 'stl_load_errors' system table for details.
    # After fetching the 'stl_load_errors' system table, I could recognized that the problem was that
    # in the task description for the purchase table one column misses, which is purchased_at.
    # Therefore I added this column in my configuration yaml file for the purchase table.

