# from_kafka_to_polaris
This is a basic example of a pipeline that consumes from a **kafka** topic, does basic data enhancement in **spark** and then uses the Polaris Push API to stream the data into a Polaris table.

The main spark application code is in **spark_consumer.py** using Polaris Authentication and Push API that are handled in the **imply_sdk.py** code.  

Additional jars that this code depends on are also included:
- kafka-clients-3.0.1.jar  
- spark-token-provider-kafka-0-10_2.12-3.1.3.jar 
- spark-sql-kafka-0-10_2.12-3.1.3.jar  
- commons-pool2-2.10.0.jar 

Also included a copy of the **meetup_events.json** file that was used as a source of data for this exercise.
