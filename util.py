import gzip
from io import BytesIO, TextIOWrapper
import boto3
import pymysql
from logger_conf import get_module_logger

logger = get_module_logger(__name__)

class UtilClass(object):
    def __init__(self):
        self.CONNECT_TIMEOUT_SECONDS = 30
        self.READ_TIMEOUT_SECONDS = 3600

    def initalise_mysql(self,config):
        """Initalises and returns a MySQL database based on config"""
        return pymysql.connect(
            host=config["MYSQL_HOST"],
            user=config["MYSQL_USERNAME"],
            password=config["MYSQL_PASSWORD"],
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout= self.CONNECT_TIMEOUT_SECONDS,
            db=config["MYSQL_DB"])


    def mysql_connection(self,connection):
        """SET few params with MySQL connection"""
        warnings = []
        with connection.cursor() as cur:
            try:
                cur.execute('SET @@session.time_zone="+0:00"')
            except Exception as e:
                warnings.append('Could not set session.time_zone. Error: ({}) {}'.format(*e.args))
            try:
                cur.execute('SET @@session.wait_timeout=2700')
            except Exception as e:
                warnings.append('Could not set session.wait_timeout. Error: ({}) {}'.format(*e.args))
            try:
                cur.execute("SET @@session.net_read_timeout={}".format(self.READ_TIMEOUT_SECONDS))
            except Exception as e:
                warnings.append('Could not set session.net_read_timeout. Error: ({}) {}'.format(*e.args))
            try:
                cur.execute('SET @@session.innodb_lock_wait_timeout=2700')
            except Exception as e:
                warnings.append(
                    'Could not set session.innodb_lock_wait_timeout. Error: ({}) {}'.format(*e.args)
                )
            if warnings:
                logger.info(("Encountered non-fatal errors when configuring MySQL session that could "
                             "impact performance:"))
            for w in warnings:
                logger.warning(w)

        return connection

    def _execute_mysql_query(self,query,cursor):
        """exectues a given sql, pymysql cursor and type"""
        try:
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            print("Error while executing query",e)


    def extract_data(self,mysql_cursor, table_spec, frequency='daily'):
        """Given a cursor, Extracts data from MySQL movielens dataset
        and returns all the tables with their data"""
        try:
            if table_spec["STRATEGY"] == 'fullload':
                query = 'select {} from {}'.format(table_spec["SELECT_COLS"], table_spec["TABLE_NAME"])
                logger.debug(query)
                result = self._execute_mysql_query(query, mysql_cursor)
            elif table_spec["STRATEGY"] == 'incremental' and frequency == 'hourly':
                query = 'select {} from {} where {} between now() - interval {} hour AND NOW()'.format(
                    table_spec["SELECT_COLS"], table_spec["TABLE_NAME"], table_spec["CDC_COLUMNS"], 1)
                logger.debug(query)
                result = self._execute_mysql_query(query, mysql_cursor)
            elif table_spec["STRATEGY"] == 'incremental' and frequency == 'daily':
                query = 'select {} from {} where {} between now() - interval {} day AND NOW()'.format(
                    table_spec["SELECT_COLS"], table_spec["TABLE_NAME"], table_spec["CDC_COLUMNS"], 1)
                logger.debug(query)
                result = self._execute_mysql_query(query, mysql_cursor)
            return result

        except Exception as e:
            logger.error("As of now we only support Strategy as Full load, Incremental load" \
                         " And hourly/incremental frequency" \
                         " Please enter the valid strategy or frequency or get in touch with DE Team", e)

    def backfill(self,mysql_cursor,hour,table_spec):
        try:
            query = 'select {} from {} where {} between now() - interval {} hour AND NOW()'.format(
                table_spec["SELECT_COLS"], table_spec["TABLE_NAME"], table_spec["CDC_COLUMNS"], hour)
            logger.debug(query)
            result = self._execute_mysql_query(query, mysql_cursor)
            return result
        except Exception as e:
            logger.error("Error while Backfilling", e)

    def checkLag(self,mysql_cursor):
        """Check if there is a log in Replica"""
        try:
            query = "SHOW SLAVE STATUS"
            logger.info("Executing query to find replica lag: $query")
            rs = self._execute_mysql_query(query,mysql_cursor)
            lag = rs["Seconds_Behind_Master"]
            logger.info("Replica lag value :%s", lag)
            if lag == '':
                slaveSQLRunning = rs["Slave_SQL_Running"]
                slaveIORunning = rs["Slave_IO_Running"]
                logger.error("Status of Slave_SQL_Running : %s", slaveSQLRunning)
                logger.error("Slave_IO_Running : %s", slaveIORunning)
        except Exception as e:
            logger.error(
                "Could not determine the replica lag because lag value is null. Either the DB provided is a master db or Replication might be broken.",
                e)


    def write_df_as_csv(self,bucket_name, file_name, df, separator=','):
        """Write the DF to S3"""
        if df.empty:  # empty dataframe - nothing to write
            return
        else:
            try:
                gz_buffer = BytesIO()
                with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
                    df.to_csv(TextIOWrapper(gz_file, 'utf8'), index=False, sep=separator)
                s3_resource = boto3.resource('s3')
                s3_object = s3_resource.Object(bucket_name, file_name)
                s3_object.put(Body=gz_buffer.getvalue())
            except Exception as e:
                logger.error("Encountered exception while writing to s3", e)
