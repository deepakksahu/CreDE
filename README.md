# CreDE
A Data Engineering assignment

##### Install pip requirements
```sh
pip install - r requirements.txt
```
##### DDL
```SQL
CREATE TABLE `currency` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `date` int(11) DEFAULT NULL,
  `base_currency` varchar(10) DEFAULT NULL,
  `target_currency` varchar(10) DEFAULT NULL,
  `rates` float DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=685 DEFAULT CHARSET=latin1
```
##### common config which is parameter to the pipeline script
```
config = json.dumps({
  "MYSQL_HOST": "localhost",
  "MYSQL_DB": "cred",
  "MYSQL_USERNAME": "root",
  "MYSQL_PASSWORD": "password",
  "TABLE_SPEC": [
    {
      "TABLE_NAME": "currency",
      "SELECT_COLS": "'rates','created_at'",
      "CDC_COLUMNS": [
        "rates",
        "created_at"
      ],
      "STRATEGY": "fullload"
    },
    {
      "TABLE_NAME": "dept",
      "SELECT_COLS": "'deptno','dname'",
      "CDC_COLUMNS": [
        "dname",
        "deptno"
      ],
      "STRATEGY": "fullload"
    }
  ],
  "S3_OUTPUT_LOCATION": "s3://everythingtest/cred/",
  "CSV_DELIM": ","
})
```

```python
python main.py --config '{ "MYSQL_HOST": "localhost", "MYSQL_DB": "cred", "MYSQL_USERNAME": "root", "MYSQL_PASSWORD": "password", "TABLE_SPEC": [ { "TABLE_NAME": "currency", "SELECT_COLS": "'rates','created_at'", "CDC_COLUMNS": [ "rates", "created_at" ], "STRATEGY": "fullload" }, { "TABLE_NAME": "dept", "SELECT_COLS": "'deptno','dname'", "CDC_COLUMNS": [ "dname", "deptno" ], "STRATEGY": "fullload" } ], "S3_OUTPUT_LOCATION": "s3://everythingtest/cred/", "CSV_DELIM": "," }'
```
