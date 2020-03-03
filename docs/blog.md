# Data ingestion in Big Query without the headaches

One of the problems with data injections is that plans change when it comes to the schema. Asking your DBA in the old days to migrate your columns always resulted in the largest of eye rolls. You want me to do what? and why? They'd ask, obviously they would give you a long turn around on making that simple change to the data structure just so you could have an additional field.

What if you could perform data ingestion without worrying about the evolution of schema and not worry about adding new data sources? Sounds like paradise, no? Then step right up and try my new data ingestion framework tool written for Cloud Dataflow and Google BigQuery.

![](SchemaMigrator.png?raw=true)

This tool can create tables automatically based on a predefined key in your json schema and it can modify the schema of those tables or pre-existing ones on the fly. What’s the catch you ask; well to start with this pipeline can only deal with flat JSON object schemas, that means no nested objects or arrays in your JSON objects, and it can only gently evolve the schema, by that I mean add additional columns. Additional functionality will be added if there is a case for it, but for now it will remain simple and generic.

The overview of the ingestion framework is is as follows, a PubSub topic with a Subscriber of the same name at the top, followed by a Cloud Dataflow pipeline and of course Google BigQuery. All of these tools scale very well and should be able to handle a large amount of data ingestion.

![](SchemaMigratorDAG.png?raw=true)

The magic happens inside the Cloud Dataflow pipeline. Here we first check that the target Dataset in Google BigQuery exists, if it does not we create it. Next we pull down JSON data from PubSub and ensure it is valid JSON, if it is not it is discarded. Then we attempt and insert into Google BigQuery. If the target table does not exist it is created, with a singly columned Schema, that of the JSON key for the destination table. In this way we ensure that the table can exist, without knowing the full schema ahead of time. If the schema matches then the data is inserted, end of story. 

Bear in mind these first steps all happen in parallel so any number of tables can be created or inserted into at once. That is the power of Dynamic Destinations in Cloud Dataflow.

Next step is to pick up all the inserts that failed, and sort/combine the schema changes into a key value pairs. This is done on a configurable 1 ish minute basis. The reason for this is to prevent overloading Google BigQuery with schema changes. Google BigQuery is not relational database and it should not be treated as such. Once we have gathered up all the schema different incoming schema changes for each target table, we then ask Google BigQuery for the actual schemas for that table and merge them. The magic update to BigQuery is made here. 

As the above step is happening the same data that triggered this change, pre aggregation into the schema key value pairs, is sent back to PubSub so it can flow through the pipeline again. This recursive nature allows the pipeline to, in streaming framework, migrate the schema of BigQuery.

Example workflow

![](simple_json.png?raw=true)

Schema initially

![](simple_schema_init.png?raw=true)


New incoming live data

![](simple_json_add_field.png?raw=true)

Schema after migration

![](simple_schema_final.png?raw=true)


The more astute of you will have noticed a problem, what happens if there is a conflict? And the schemas can not be migrated? Yes, then the data will be stuck forever going around and around through PubSub, failing be inserted and attempting to migrate the schema and then being sent back to PubSub again. In future versions this will be addressed with a bad data table for failures. So it is easy to find out where and why that specific data was not inserted.

More information can be found at the GitHub repo for this [project](https://github.com/doitintl/dataflow-bigquery-schema-migrator-insert). A big shout-out to DoiT International, for letting me open source this project and giving me feedback.
