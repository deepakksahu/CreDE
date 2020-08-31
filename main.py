import argparse
import calendar
import json
import time
from util import UtilClass
import pandas as pd

from logger_conf import get_module_logger

logger = get_module_logger(__name__)

# Done
# MySQL Version: 5.6+
# Output Format: csv
# Compression: gz
# Output Storage: Local files or S3 (Preferred)
# Delimiter: Configurable
# Table names: Multiple, Configurable
# Select Columns: Multiple, Configurable
# Externalised configuration, to support multiple tables
# Frequency: Configurable (ex: hourly)
# CDC Columns: Multiple, Configurable (ex: created_time, updated_time)--Single
# Time Range: Configurable (ex: 12 hours == fetches data for the last 12 hours)

# ToDO
# Must have:
# Daemon process that allows clean start and shutdown (and not kill -9 :)
# Support for sliding window based extractions
# Example: Bootstrapped a table to extract last 12 hrs data at 8am (8pm to 8am), set at
# hourly frequency. Next time the job runs for the table, it should pick up data from (9pm to
# 9am).
# Some of the columns in mysql have JSON in it, which needs to be split into multiple keys as per the need. The pipeline should have a transformation layer to handle such scenario


# ToDO
# Nice to have:
# Streaming to s3 directly (use AWS free tier)
# Auto-recovery - If an instance of extraction fails, include the failed window when it runs next
# Python is preferred, but any language can be used.

# python main.py --config '{ "MYSQL_HOST": "localhost", "MYSQL_DB": "cred", "MYSQL_USERNAME": "root", "MYSQL_PASSWORD": "password", "TABLE_SPEC": [ { "TABLE_NAME": "currency", "SELECT_COLS": "'rates','created_at'", "CDC_COLUMNS": "created_at", "STRATEGY": "incremental"}, { "TABLE_NAME": "dept", "SELECT_COLS": "'deptno','dname'", "CDC_COLUMNS": "dname", "STRATEGY": "fullload" } ],"FREQUENCY":"daily","S3_BUCKET":"everythingtest","OUTPUT_FOLDER_LOCATION": "cred/currency/", "CSV_DELIM": "," }'

config = """{
"MYSQL_HOST":"localhost",
"MYSQL_DB":"cred",
"MYSQL_USERNAME":"root",
"MYSQL_PASSWORD":"password",
"TABLE_SPEC": [{"TABLE_NAME":"","SELECT_COLS":"'',''","CDC_COLUMNS": '',"STRATEGY":"incremental/fullload"}],
"FREQUENCY":"daily/hourly" 
"S3_BUCKET":''
"OUTPUT_FOLDER_LOCATION":""
"CSV_DELIM":","
}"""



def main():
    util = UtilClass()
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, required=True)
    args = parser.parse_args()
    logger.debug("Done with all the config collection")
    config = json.loads(args.config)
    logger.debug("Config: %s", json.dumps(config, indent=2))

    separator = config['CSV_DELIM']
    s3_bucket = config["S3_BUCKET"]
    output_folder_location = config['OUTPUT_FOLDER_LOCATION']
    frequency = config['FREQUENCY']
    table_specs = config['TABLE_SPEC']

    logger.info('===============================Starting data pipeline===============================')
    logger.info('===============================Initialising MySQL connection========================')
    mysql_conn = util.initalise_mysql(config)
    mysql = util.mysql_connection(mysql_conn)
    mysql_cursor = mysql.cursor()
    #util.checkLag(mysql_cursor)  # Comment this line if you are testing on local or no replica setup
    logger.info('===============================MySQL connection created successfully ===============')
    for table_spec in table_specs:
        mysql_data = util.extract_data(mysql_cursor, table_spec, frequency)
        df_mysql_data = pd.DataFrame(mysql_data)
        file_name = output_folder_location + '/' + table_spec["TABLE_NAME"] + '_' + str(
            calendar.timegm(time.gmtime())) + '.csv.gz'
        if len(df_mysql_data) > 0 and s3_bucket != "":
            util.write_df_as_csv(s3_bucket, file_name, df_mysql_data, separator)
    mysql.close()
    logger.info('===============================MySQL connection Closed===============================')

if __name__ == '__main__':
    main()
