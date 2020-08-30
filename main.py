import argparse
import json
import os
import pandas as pd
import pymysql

from logger_conf import get_module_logger

logger = get_module_logger(__name__)

#Done
# MySQL Version: 5.6+
# Output Format: csv
# Compression: gz
# Output Storage: Local files or S3 (Preferred)
# Delimiter: Configurable
# Table names: Multiple, Configurable
# Select Columns: Multiple, Configurable



#ToDO
# CDC Columns: Multiple, Configurable (ex: created_time, updated_time)
# Frequency: Configurable (ex: hourly)
# Time Range: Configurable (ex: 12 hours == fetches data for the last 12 hours)
# Must have:
# Externalised configuration, to support multiple tables
# Daemon process that allows clean start and shutdown (and not kill -9 :)
# Support for sliding window based extractions
# Example: Bootstrapped a table to extract last 12 hrs data at 8am (8pm to 8am), set at
# hourly frequency. Next time the job runs for the table, it should pick up data from (9pm to
# 9am).
# Some of the columns in mysql have JSON in it, which needs to be split into multiple keys as per the need. The pipeline should have a transformation layer to handle such scenario

#ToDO
# Nice to have:
# Streaming to s3 directly (use AWS free tier)
# Auto-recovery - If an instance of extraction fails, include the failed window when it runs next
# Python is preferred, but any language can be used.

#python main.py --config '{ "MYSQL_HOST": "localhost", "MYSQL_DB": "cred", "MYSQL_USERNAME": "root", "MYSQL_PASSWORD": "password", "TABLE_SPEC": [ { "TABLE_NAME": "currency", "SELECT_COLS": "'rates','created_at'", "CDC_COLUMNS": [ "rates", "created_at" ], "STRATEGY": "fullload" }, { "TABLE_NAME": "dept", "SELECT_COLS": "'deptno','dname'", "CDC_COLUMNS": [ "dname", "deptno" ], "STRATEGY": "fullload" } ], "S3_OUTPUT_LOCATION": "s3://everythingtest/cred/", "CSV_DELIM": "," }'

# {
#   "MYSQL_HOST": "localhost",
#   "MYSQL_DB": "cred",
#   "MYSQL_USERNAME": "root",
#   "MYSQL_PASSWORD": "password",
#   "TABLE_SPEC": [
#     {
#       "TABLE_NAME": "currency",
#       "SELECT_COLS": "'rates','created_at'",
#       "CDC_COLUMNS": [
#         "rates",
#         "created_at"
#       ],
#       "STRATEGY": "fullload"
#     },
#     {
#       "TABLE_NAME": "dept",
#       "SELECT_COLS": "'deptno','dname'",
#       "CDC_COLUMNS": [
#         "dname",
#         "deptno"
#       ],
#       "STRATEGY": "fullload"
#     }
#   ],
#   "S3_OUTPUT_LOCATION": "s3://everythingtest/cred/",
#   "CSV_DELIM": ","
# }


config = """{
"MYSQL_HOST":"localhost",
"MYSQL_DB":"cred",
"MYSQL_USERNAME":"root",
"MYSQL_PASSWORD":"password",
"TABLE_SPEC": [{"TABLE_NAME":"","SELECT_COLS":"'',''","CDC_COLUMNS": ['',''],"STRATEGY":"incremental/fullload"}],
"CORN_EXPR":"5 4 * * *",
"S3_OUTPUT_LOCATION":""
"CSV_DELIM":","
}"""

backfill_config = """{"TIME_RANGE_HOURS":12 }"""


def initalise_mysql(config):
    """Initalises and returns a MySQL database based on config"""
    return pymysql.connect(
        host=config["MYSQL_HOST"],
        user=config["MYSQL_USERNAME"],
        password=config["MYSQL_PASSWORD"],
        cursorclass=pymysql.cursors.DictCursor,
        db=config["MYSQL_DB"])


def execute_mysql_query(sql, cursor, query_type):
    """exectues a given sql, pymysql cursor and type"""
    if query_type == "fetchall":
        cursor.execute(sql)
        return cursor.fetchall()
    elif query_type == "fetchone":
        cursor.execute(sql)
        return cursor.fetchall()
    else:
        pass


def execute_mysql_query(sql, cursor, query_type):
    """exectues a given sql, pymysql cursor and type"""
    if query_type == "fetchall":
        cursor.execute(sql)
        return cursor.fetchall()
    elif query_type == "fetchone":
        cursor.execute(sql)
        return cursor.fetchall()
    else:
        pass


def extract_data(mysql_cursor,table_spec):
    """Given a cursor, Extracts data from MySQL movielens dataset
    and returns all the tables with their data"""
    try:
        if table_spec["STRATEGY"]=='fullload':
            query = 'select {} from {}'.format(table_spec["SELECT_COLS"],table_spec["TABLE_NAME"])
            result = execute_mysql_query(query, mysql_cursor, 'fetchall')
        elif table_spec["STRATEGY"]=='incremental':
            query = 'select {} from {}'.format(table_spec["SELECT_COLS"],table_spec["TABLE_NAME"])
            result = execute_mysql_query(query, mysql_cursor, 'fetchall')
        return result
    except Exception as e:
        logger.error("As of now we only support Strategy as Full load, Incremental load"\
                  "Please enter the valid strategy or get in touch with DE Team",e)


def checkLag(mysql_cursor):
    try:
        query = "SHOW SLAVE STATUS"
        logger.info("Executing query to find replica lag: $query")
        rs = execute_mysql_query(query)
        lag = rs["Seconds_Behind_Master"]
        logger.info("Replica lag value :%s", lag)
        if lag == '':
            slaveSQLRunning = rs["Slave_SQL_Running"]
            slaveIORunning = rs["Slave_IO_Running"]
            logger.error("Status of Slave_SQL_Running : %s", slaveSQLRunning)
            logger.error("Slave_IO_Running : %s", slaveIORunning)
    except Exception as e:
        print("Could not determine the replica lag because lag value is null. Replication might be broken.", e)


def write_df_as_csv(s3_path, df, separator=',', ):
    if df.empty:  # empty dataframe - nothing to write
        return
    else:
        df.to_csv(s3_path, index=False, sep=separator, compression='gzip')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, required=True)
    args = parser.parse_args()
    logger.debug("Done with all the config collection")

    config = json.loads(args.config)
    logger.debug("Config: %s", json.dumps(config, indent=2))
    separator = config['CSV_DELIM']
    s3_out_location = config['S3_OUTPUT_LOCATION']


    print('Starting data pipeline')
    print('Initialising MySQL connection')
    mysql = initalise_mysql(config)
    print('MySQL connection Completed')
    mysql_cursor = mysql.cursor()
    table_specs = config['TABLE_SPEC']
    for table_spec in table_specs:
        mysql_data = extract_data(mysql_cursor,table_spec)
        df_mysql_data = pd.DataFrame(mysql_data)
        print(df_mysql_data)
    s3_path_mysql_data = os.path.join(s3_out_location, 'tablename/filename.csv')

    # write_df_as_csv(s3_path_mysql_data,df_mysql_data,separator)

    print(df_mysql_data)


main()
