# Kafka2BigQuery

This repository contains a Java codebase that implements an Apache Beam pipeline designed to seamlessly process and ingest events from a Kafka cluster and store them in a BigQuery table. The pipeline serves as a robust and scalable solution for real-time data ingestion and analysis.

To build the project
```bash
./gradlew shadowJar
```

To run the object
```bash
java -jar build/libs/datalake-consumer-1.0-SNAPSHOT-all.jar \
		--runner=DataflowRunner \
		--appName= \
		--jobName= \
		--project= \
		--region=us-east1 \
		--stagingLocation= \
		--gcpTempLocation= \
		--network= \
		--subnetwork= \
		--logLevel=INFO \
		--kafkaHost= \
		--kafkaUser= \
		--kafkaPassword= \
		--registryHost= \
		--registryUser= \
		--registryPassword= \
		--kafkaTopic= \
		--consumerGroup= \
		--datalake= \
		--dataset= \
		--tableName=
```
