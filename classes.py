# Python, version: 3.6.1
# psycopg2, version: 2.8.4
# pandas, version: 0.25.1
import os
import sys
import psycopg2
import pandas as pd


class Interface_s3_redshift:
    """
    Class for building of the interface between S3 and Amazon Redshift
    and data engineering in redshift database.
    """
    def __init__(self, dbname, host, port, 
                 user, password, schema,
                 s3_tables_csv, aws_access_key_id, aws_secret_access_key, aws_bucket):
        """
        Initialization of class attributes.
        :param dbname: string
        Redshift database name
        :param host: string
        Redshift database host
        :param port: string
        Redshift database port
        :param user: string
        Redshift database user
        :param password: string
        Redshift database password
        :param schema: string
        Redshift database schema
        :param s3_tables_csv: dict of dicts
        Key is the name of the S3 table and value is dictionary
        where the key is the name of column name and value is data type
        :param aws_access_key_id: string
        S3 access key id
        :param aws_secret_access_key: string
        S3 secret access key id
        :param aws_bucket: string
        S3 bucket name
        """
        self.dbname = dbname
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.schema = schema
        self.s3_tables_csv = s3_tables_csv
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_bucket = aws_bucket

    def connect_db(self):
        """
        It defines the connection string to the Redshift database
        and connects to the Redshift database.
        """
        self.conn_string = """dbname={dbname} port={port} 
                             user={user} password={password} host={host}""".format(
            dbname=self.dbname, port=self.port,
            user=self.user, password=self.password, host=self.host)
        try:
            self.conn = psycopg2.connect(self.conn_string)
        except psycopg2.OperationalError as error:
            print('Unable to connect to Redshift!')
            print(error)
            sys.exit(1)
        else:
            print("Connection Successful to Redshift!")
            self.cur = self.conn.cursor()

    def execute_query(self, sql):
        """
        It executes the query string.
        :param sql: string
        Query string.
        """
        try:
            self.cur.execute(sql)
        except psycopg2.OperationalError as error:
            print('Unable to execute query!')
            print(error)
            sys.exit(1)

    def from_csv_to_table_name(self):
        """
        It creates a dictionary where the key is the S3 csv file
        and value is the Redshift table name.
        """
        self.dict_s3_table = {csv_file: csv_file.split('.')[0]
                              for csv_file in self.s3_tables_csv}

    def get_cols_table(self):
        """
        It concatenates the table's columns name with its date type.
        The columns are separated by a comma.
        """
        self.cols_str = ",".join(key + ' ' + self.s3_tables_csv[self.csv_file][key]
                       for key in self.s3_tables_csv[self.csv_file])

    def create_table(self):
        """
        It checks whether the table already exists in the Redshift database;
        if true, then it drops the table.
        Afterwards it creates a a new table with the specified columns name and data type.
        """
        sql_drop_if_exists = """ DROP TABLE IF EXISTS {schema}.{name_table};""".format(
                    schema=self.schema,
                    name_table=self.dict_s3_table[self.csv_file])
        sql_create = """CREATE TABLE {schema}.{name_table} ({columns});""" \
            .format(schema=self.schema,
                    name_table=self.dict_s3_table[self.csv_file],
                    columns=self.cols_str)
        self.execute_query(sql=sql_drop_if_exists)
        self.execute_query(sql=sql_create)

    def copy_table_from_s3_to_redshift(self):
        """
        It loads the S3 csv file to the Redshift table.
        """
        sql_copy = """
        copy {schema}.{name_table} from 's3://{aws_bucket}/{csv_file}'
        credentials 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}' IGNOREHEADER 1
        csv timeformat 'YYYY-MM-DD HH24:MI:SS'; """.format(
        schema=self.schema,
        name_table=self.dict_s3_table[self.csv_file],
        aws_bucket=self.aws_bucket,
        csv_file=self.csv_file,
        aws_access_key_id=self.aws_access_key_id,
        aws_secret_access_key=self.aws_secret_access_key)
        self.execute_query(sql=sql_copy)

    def create_lookup_trial_subscription(self, table_name=None):
        """
        It executes a full join between two tables and creates the Redshift table:
        - selected data from PURCHASES Redshift table filtered by purchase_type equal to "sale"
        - selected data from PURCHASES Redshift table filtered by purchase_type equal to "trial"
        :param table_name: string
        Table of the created table
        """
        self.table_trial_subs = "LOOKUP_TRIAL_SUBSCRIPTION" if table_name is None else table_name
        sql_merge3 = """ CREATE TABLE {schema}.{table_trial_subs} AS
                     SELECT 
                     CASE WHEN T1.USER_ID IS NOT NULL
                        THEN T1.USER_ID
                        ELSE T_TRIAL.USER_ID
                        END AS USER_ID
                    , T1.PLAN_ID AS PLAN_ID_SUB
                    , T_TRIAL.PLAN_ID AS PLAN_ID_TRIAL
                    , CASE 
                        WHEN T_TRIAL.PURCHASE_TYPE IS NOT NULL
                        THEN 1
                        END AS TRIAL 
                    , CASE WHEN T1.PURCHASE_TYPE IS NOT NULL
                        THEN 1
                        END AS SUBSCRIPTION
                    , T1.PURCHASED_AT AS PURCHASED_AT_SUB
                    , T_TRIAL.PURCHASED_AT AS PURCHASED_AT_TRIAL
                    , T1.AMOUNT
                    FROM (SELECT * FROM {schema}.{purchase_table}
                    WHERE PURCHASE_TYPE='sale') AS T1
                    FULL JOIN (SELECT * FROM {schema}.{purchase_table}
                    WHERE PURCHASE_TYPE='trial') AS T_TRIAL
                    ON T1.USER_ID = T_TRIAL.USER_ID;
                    """.format(
        schema=self.schema,
        table_trial_subs=self.table_trial_subs,
        purchase_table="PURCHASES")
        self.execute_query(sql=sql_merge3)

    def create_export_table(self, table_name=None):
        """
        It executes a left join between the A/B test assignment's table
        and purchase and users Redshift tables.
        :param table_name: string
        Table of the created table
        """
        self.export_table = "EXPORT_TABLE" if table_name is None else table_name
        sql_final_table = """CREATE TABLE {schema}.{export_table} AS
                         SELECT T1.USER_ID
                        , MAX(T1.ASSIGNED_AT) OVER(PARTITION BY T1.VARIANT) AS END_VARIANT
                        , MIN(T1.ASSIGNED_AT) OVER(PARTITION BY T1.VARIANT) AS START_VARIANT
                        , T1.VARIANT
                        , T2.SIGNUP_DATE
                        , T3.TRIAL
                        , T3.PLAN_ID_TRIAL
                        , T3.SUBSCRIPTION
                        , T3.PLAN_ID_SUB
                        , T3.PURCHASED_AT_TRIAL
                        , T3.PURCHASED_AT_SUB
                        , T3.AMOUNT
                        FROM {schema}.{abtest_table} AS T1
                        LEFT JOIN {schema}.{users_table} AS T2
                        ON T1.USER_ID = T2.USER_ID
                        LEFT JOIN {schema}.{lookup_subscr_table} AS T3
                        ON T1.USER_ID = T3.USER_ID;
                        """.format(
            schema=self.schema,
            export_table=self.export_table,
            lookup_subscr_table=self.table_trial_subs,
            users_table="USERS",
            abtest_table="ABTEST_ASSIGNMENTS")
        self.execute_query(sql=sql_final_table)

    def load_db_from_s3_to_redshift(self):
        """
        It connects to the Redshift database and afterwards iterates
        through the S3 csv files, creating the respective table in the Redshift database
        and copying the data to the created table.
        """
        self.connect_db()
        self.from_csv_to_table_name()
        for csv_file in self.s3_tables_csv:
            self.csv_file = csv_file
            self.get_cols_table()
            self.create_table()
            self.copy_table_from_s3_to_redshift()
            print("Successfully loaded csv file: {csv_file} from s3 to Redshift table: {name_table}"
                  .format(name_table=self.dict_s3_table[self.csv_file],
                          csv_file=self.csv_file))

    def join_tables_redshift_db(self):
        """
        It creates a new table from the full join of the purchase table
        and left join between the A/B test assignment's table
        and purchase and users Redshift tables.
        Afterwards it exports the data from the Redshift export table
        and saves it locally in a csv file.
        """
        self.create_lookup_trial_subscription()
        self.create_export_table()
        self.df = pd.read_sql("SELECT * FROM {schema}.{export_table};"
                              .format(schema=self.schema, export_table=self.export_table), con=self.conn)
        file_path = os.path.join(os.getcwd(), 'final_redshift_table.csv')
        self.df.to_csv(file_path, index=False)
        print("Successfully imported {export_table} "
              "and saved locally under the following path: {file_path} "
              .format(export_table=self.export_table,
                      file_path=file_path))