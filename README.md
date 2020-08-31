# CreDE
A Data Engineering assignment

**Approach:**
1. Create a util class which will consist of all the functions.
2. In the **main.py** create an object for the class and use the same
   for any call.
3. Modularise the code according to their functioning.
4. Add logging for better debugging.
5. Add required comments to increase the readability of the code.
6. Add the docker file to containerize the solution.
7. Add all the dependency in requirements.txt.
8. Write the DF to s3.
9. Add the airflow DAG consider we have k8s to spin pods.
10.In the **backfill.py** create an object for the class and use the same
   for any call it takes hour to look back and take the delta data and  
   create a csv out of it.

**Some of the columns in mysql have JSON in it, which needs to be split  
into multiple keys as per the need. The pipeline should have a
transformation layer to handle such scenario**

For the above Question we have to get the data in a DF and parse it  
accordingly. **data_ingestor.py** is doing one such parsing of the data.  
Though this has been used to populate the source table.


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
```sh
config = json.dumps({
  "MYSQL_HOST": "localhost",
  "MYSQL_USERNAME": "root",
  "MYSQL_PASSWORD": "password",
  "TABLE_SPEC": [
    {
      "MYSQL_DB": "cred",
      "TABLE_NAME": "currency",
      "SELECT_COLS": "'rates','created_at'",
      "CDC_COLUMNS": "created_at",
      "STRATEGY": "incremental"
    },
    {
      "MYSQL_DB": "cred",
      "TABLE_NAME": "dept",
      "SELECT_COLS": "'deptno','dname'",
      "CDC_COLUMNS": "dname",
      "STRATEGY": "fullload"
    }
  ],
  "FREQUENCY": "daily",
  "S3_BUCKET": "everythingtest",
  "OUTPUT_FOLDER_LOCATION": "cred",
  "CSV_DELIM": ","
})
```

##### Config to the backfill Script
```sh
backfill_config = json.dumps({
  "MYSQL_HOST": "localhost",
  "MYSQL_USERNAME": "root",
  "MYSQL_PASSWORD": "password",
  "TABLE_SPEC": [
    {
      "MYSQL_DB": "cred",
      "TABLE_NAME": "currency",
      "SELECT_COLS": "'rates','created_at'",
      "CDC_COLUMNS": "created_at"
    }
  ],
  "TIME_RANGE_HOURS": 12,
  "S3_BUCKET": "everythingtest",
  "OUTPUT_FOLDER_LOCATION": "cred",
  "CSV_DELIM": ","
})
```
##### Command to create pipeline
```python
python main.py --config '{ "MYSQL_HOST": "localhost",  "MYSQL_USERNAME": "root", "MYSQL_PASSWORD": "password", "TABLE_SPEC": [ { "MYSQL_DB": "cred","TABLE_NAME": "currency", "SELECT_COLS": "'rates','created_at'", "CDC_COLUMNS": "created_at", "STRATEGY": "incremental"}, {"MYSQL_DB": "cred", "TABLE_NAME": "dept", "SELECT_COLS": "'deptno','dname'", "CDC_COLUMNS": "dname", "STRATEGY": "fullload" } ],"FREQUENCY":"daily","S3_BUCKET": "everythingtest","OUTPUT_FOLDER_LOCATION":"cred", "CSV_DELIM": "," }'
```
##### Command to backfill
```python
python backfill.py --backfill_config '{ "MYSQL_HOST": "localhost", "MYSQL_USERNAME": "root", "MYSQL_PASSWORD": "password", "TABLE_SPEC": [ {  "MYSQL_DB": "cred","TABLE_NAME": "currency", "SELECT_COLS": "'rates','created_at'", "CDC_COLUMNS": "created_at" } ], "TIME_RANGE_HOURS": 12, "S3_BUCKET": "everythingtest", "OUTPUT_FOLDER_LOCATION": "cred", "CSV_DELIM": "," }'
```

##### Output
```sh
s3://everythingtest/cred/
```
