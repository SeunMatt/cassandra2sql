Cassandra2SQL
==============

[![Build Status](https://travis-ci.org/SeunMatt/cassandra2sql.svg?branch=master)](https://travis-ci.org/SeunMatt/cassandra2sql)

This is a simple library for exporting cassandra database to SQL that can be used to restore an SQL database like MySQL and Postgres.

It handles the database columns based on their types i.e. TEXT columns are processed differently from TIMESTAMP type of columns.

**Every collection columns, by default, are converted into JSON string**


Usage
=====

Requirements
------------
Installed [JRE 8+](https://www.oracle.com/technetwork/java/javase/downloads/jre8-downloads-2133155.html)

Verify your installation by running `java -version`  

Running/Executing the Tool
---------------------------
From the command line execute `java -jar ./dist/cassandra2sql-1.0.jar application.properties`

**Where `application.properties` is the full path to the **.properties** config file**

Configuration
--------------
Cassandra2SQL uses `application.properties` file as a config source. It's a simple KEY=VALUE entry config file with a `.properties` extension. So the file can have any name other than `application` just supply it to the app during execution

**TABLE CONFIGURATION**

In the config file we can specify the cassandra (source) table we want to export as SQL in the following format:

`TABLES=srcTableName1:targetTableName1,srcTableName2:targetTableName2, . . .`

The `srcTableName` is the name of the table to be exported in cassandra, while the `targetTableName` is the corresponding
table name on the SQL-database (MYSQL/POSTGRES). Multiple tables can be separated with a comma `,`

For example, this config will instruct the tool to export the tables `user` as `users`, `sentmessage` as `sent_messages` and so on.

 `TABLES=user:users,sentmessage:sent_messages,logs:logs`
 
 The SQL insert statement generated will look like `INSERT INTO users ...`, `INSERT INTO sent_messages ...` and so on.

**COLUMN CONFIGURATION**

Next is the column configuration for each tables. The configuration is under the key `TABLE_CONFIG_SRCTABLE`. It's a combination of the uppercase format of the `srcTableName` supplied in the `TABLES` section. 

The value of each table config is the source column name and the target column name separated by the `:` symbol. 

Thus, we can config columns for the `users` table defined above:

`TABLE_CONFIG_USER=firstName:first_name,bvnverified:bvn_verified,emailverified:email_verified,id=:id`

For us to configure columns for the logs table, we just need to add this entry to the `application.properties` file:

`TABLE_CONFIG_LOGS=data:data,createdat=created_at,action=action_log`

So if we defined 10 tables in the `TABLES` section, it means we'll have 10 `TABLE_CONFIG_XXX` in the configuration file.

**More Configuration Properties**

```properties
#option to either delete the generated zip file or not, default is false
DELETE_GENERATED_ZIPFILE=false

#option to keep the generated sql, default is false as this can lead large memory consumption
KEEP_GENERATED_SQL=false

#the keyspace to connect to
KEYSPACE=claneapp

#the target SQL database. Available options are: POSTGRES or MYSQL
SQL_FLAVOUR=POSTGRES

#the datetime format to use when exporting timestamp columns
DATETIME_FORMAT=yyyy-MM-dd HH:mm:ss

#the date format to use when exporting date comlumns
DATE_FORMAT=yyyy-MM-dd

#database connection configs. If not supplied, 127.0.0.1 is used as the host and 9042 as the port
#with empty username and password
DB_HOST=127.0.0.1
DB_PORT=9042
DB_USERNAME=
DB_PASSWORD=

#minimum email properties required for sending generated zip file to an email address as attachment
#only required if you want generated zip file to be sent to your email
EMAIL_HOST=smtp.mailtrap.io
EMAIL_PORT=25
EMAIL_USERNAME=your-password
EMAIL_PASSWORD=your-username
EMAIL_FROM=seun@example.com
EMAIL_TO=smatt@example.com
```

Building from Source
======================
Requirements:
-------------
- Apache Maven 3+
- JRE 8+

Execute `mvn clean package` and the binary file will be available in `target/cassandra2sql-1.0.jar`. 

To skip tests execute `mvn clean package -DskipTests=true`


LICENSE
=======
MIT

Author
======
[Seun Matt](https://smattme.com/about)

Contributing
=============
Found a bug? please create an issue for it with as much details as possible.

Wanna improve the codebase? PRs are welcome