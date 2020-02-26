# dataflow-bigquery-schema-migrator-insert
Dataflow Bigquery Schema Migrator Insert

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
