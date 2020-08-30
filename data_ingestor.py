import pandas as pd
import requests
import datetime
import mysql.connector

def get_python_datetime():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),datetime.datetime.now().strftime("%Y-%m-%d")

def currencyapi(run_date, app_id):
    url = "https://openexchangerates.org/api/historical/{}.json?app_id={}&base=USD".format(run_date, app_id)
    response = requests.request("GET", url)
    return response.json()

def parser(record):
    additional_names = record['rates']
    additional_names = [] if additional_names is None else additional_names
    currency_details_usd = []
    for key, value in additional_names.items():
        currency_detail_usd = [
            record['timestamp'],
            record['base'],
            key,
            value,
        ]
        currency_details_usd.append(currency_detail_usd)
    return currency_details_usd
def main():
    run_ts,run_date=get_python_datetime()
    record = currencyapi(run_date, "251803cdbb994fe2813635578dacbd0a")
    currency_details_usd = parser(record)
    pd.set_option('display.max_colwidth', -1)
    print(currency_details_usd)
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='cred',
                                             user='root',
                                             password='password')
        cursor = connection.cursor()
        for data in currency_details_usd:
            query = "insert into %s (date, base_currency, target_currency, rates)" \
                            " values(\"%s\", \"%s\", \"%s\", \"%s\")"
            args = ("currency",data[0],data[1],data[2],data[3])
            print(query % args)
            cursor.execute(query % args)
        connection.commit()
    except Exception as e:
        print("Error reading/writing data from/to MySQL table", e)
    finally:
        if (connection.is_connected()):
            connection.close()
            cursor.close()
            print("MySQL connection is closed")

if __name__ == '__main__':
    main()
#
# CREATE TABLE `currency` (
#   `id` int(11) NOT NULL AUTO_INCREMENT,
#   `date` int(11) DEFAULT NULL,
#   `base_currency` varchar(10) DEFAULT NULL,
#   `target_currency` varchar(10) DEFAULT NULL,
#   `rates` float DEFAULT NULL,
#   `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
#   `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
#   PRIMARY KEY (`id`)
# ) ENGINE=InnoDB AUTO_INCREMENT=685 DEFAULT CHARSET=latin1