# Dataflow Bigquery Schema Migrator Insert
This pipeline accepts JSON Strings from Cloud PubSub, dynamically redirects that JSON Object based on a predefined key
to a target BigQuery table, an attempt at inserting the data is made, if this fails the target table's  schema is adjusted to accomodate the incoming JSON Object such that,
it is able to be inserted.

![](docs/SchemaMigrator.png?raw=true)

Please bear in mind mulitple tables can be updated all at once, do not set the window value too low.

Example Dataflow pipeline:

![](docs/SchemaMigratorDAG.png?raw=true)

## Limitations 

* Currently on flat JSON Data is accepted, this will change in future releases, no json arrays or json objects can be accepted as keys. This is due to polymorphic json issues.

* Infinite retries, there is no limit on retries data can get stuck forever in the loop if there is an issue, in future releases all data will have a set number of retries.

## Requirements

* Java 8
* Enabled Google Bigquery API
* Enabled Google PubSub
* Create a topic and subscriber in Google Pubsub with the same name
* Permissions on both Google Bigquery and Pubsub, recommend: set GOOGLE_APPLICATION_CREDENTIALS environment variable

## How to run me

```bash
./gradlew clean shadowJar \
  && java -jar build/libs/schemamigration-1.0.0.jar \ --runner=DataflowRunner \ 
  --inputTopic=<topic and subscriber to listen to they are assumed to have the same name> \ 
  --jsonAttributeForTargetTableName=<name of table in json object>\
  --failOverTableName=<full path to failure table> \
  --targetDataset=<your dataset> \
  --project=<project name>  \
  --tempLocation=gs://<bucket-name>/staging
```

## TODO

* Accept more complex schema
* Create process to put data in fail over table
* Limit retries to 3, or some configurable number
* MOAR tests need 100%
