import argparse
import calendar
import json
import time
from util import UtilClass
import pandas as pd

from logger_conf import get_module_logger

logger = get_module_logger(__name__)

def main():
    try:
        util = UtilClass()
        parser = argparse.ArgumentParser()
        parser.add_argument('--backfill_config', type=str, required=False)
        args = parser.parse_args()
        logger.debug("Done with all the config collection")
        config = json.loads(args.backfill_config)
        logger.debug("Config: %s", json.dumps(config, indent=2))

        separator = config['CSV_DELIM']
        s3_bucket = config["S3_BUCKET"]
        output_folder_location = config['OUTPUT_FOLDER_LOCATION']
        backfill_hour = config['TIME_RANGE_HOURS']
        table_specs = config['TABLE_SPEC']

        logger.info('===============================Starting data pipeline===============================')
        logger.info('===============================Initialising MySQL connection========================')
        mysql_conn = util.initalise_mysql(config)
        mysql = util.mysql_connection(mysql_conn)
        mysql_cursor = mysql.cursor()
        #util.checkLag(mysql_cursor)  # Comment this line if you are testing on local or no replica setup
        logger.info('===============================MySQL connection created successfully ===============')
        for table_spec in table_specs:
            mysql_data = util.backfill(mysql_cursor,backfill_hour, table_spec)
            df_mysql_data = pd.DataFrame(mysql_data)
            file_name = output_folder_location + '/' + table_spec["TABLE_NAME"] + '_' + str(
                calendar.timegm(time.gmtime())) + '.csv.gz'
            if len(df_mysql_data) > 0 and s3_bucket != "":
                util.write_df_as_csv(s3_bucket, file_name, df_mysql_data, separator)

    except Exception as e:
        logger.error("Oops Error!! See below to find more",e)
    finally:
        mysql.close()
        logger.info('===============================MySQL connection Closed===============================')

if __name__ == '__main__':
    main()