package br.com.rodrigo.kafka;

import java.util.HashMap;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.kafka.ConfluentSchemaRegistryDeserializerProvider;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.SdkHarnessOptions.LogLevel;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;


public class KafkaToBigQuery {

    private static final Logger logger = LoggerFactory.getLogger(KafkaToBigQuery.class);
    public static void main(String[] args) throws InterruptedException, SchemaDefinitionNotFoundException {

        PipelineOptionsFactory.register(KafkaToBigQueryPipelineOptions.class);

        KafkaToBigQueryPipelineOptions options = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(KafkaToBigQueryPipelineOptions.class);

        options.setStreaming(true);

        options.setDefaultSdkHarnessLogLevel(LogLevel.ERROR);

        String datalake = options.getDatalake();
        String dataset = options.getDataset();
        String tableName = options.getTableName();

        BigQueryTable bigQueryTable = new BigQueryTable(datalake, dataset, tableName);

        String fullTableName = bigQueryTable.getTableName();

        while (!bigQueryTable.tableExists()) {
            logger.warn("Table " + fullTableName + " not found! Retrying in 5 minutes.");

            int sleepInMinutes = 60000 * 5; // It's going to sleep 5 minutes before retrying
            Thread.sleep(sleepInMinutes);
            bigQueryTable.refreshState();
        }

        logger.info("Table " + fullTableName + " exists. Going forward.");

        Pipeline pipeline = Pipeline.create(options);

        String kafkaHost = options.getKafkaHost();
        String kafkaUser = options.getKafkaUser();
        String kafkaPassword = options.getKafkaPassword();
        String kafkaTopic = options.getKafkaTopic();
        String offset = options.getOffset();

        String registryHost = options.getRegistryHost();
        String registryUser = options.getRegistryUser();
        String registerPassword = options.getRegistryPassword();

        String consumerGroup = options.getConsumerGroup();

        KafkaEventConfig kafkaConfig = new KafkaEventConfig(
            registryHost,
            registryUser,
            registerPassword,
            kafkaUser,
            kafkaPassword,
            kafkaTopic,
            consumerGroup,
            offset
        );

        HashMap<String, Object> schemaRegistryConfigs = kafkaConfig.getSchemaRegistryConfig();
        HashMap<String, Object> kafkaConfigs = kafkaConfig.getConsumerConfig();

        PCollection<KV<byte[], GenericRecord>> events = pipeline.apply(
            "Topic Reader",
            KafkaIO.<byte[], GenericRecord>read()
                .withBootstrapServers(kafkaHost)
                .withTopic(kafkaTopic)
                .withConsumerConfigUpdates(kafkaConfigs)
                .withKeyDeserializer(ByteArrayDeserializer.class)
                .withValueDeserializer(
                    ConfluentSchemaRegistryDeserializerProvider.of(
                        registryHost,
                        1000,
                        kafkaTopic + "-value",
                        null,
                        schemaRegistryConfigs
                    )
                )
                .commitOffsetsInFinalize()
                .withoutMetadata()
        );

        PCollection<GenericRecord> eventValues = events.apply("Values Splitter", Values.<GenericRecord>create());

        PCollection<TableRow> bigQueryRows = eventValues.apply(
            "Events to BigQuery rows",
            ParDo.of(new EventToBigQuery(new SerializableFunction<Void, BigQueryTable>() {
                @Override
                public BigQueryTable apply(Void input) {
                    return new BigQueryTable(datalake, dataset, tableName);
                }
            }))
        );

        bigQueryRows.apply("Write rows in BQ",
            BigQueryIO.writeTableRows()
                .to(bigQueryTable.getTableReference())
                .withSchema(bigQueryTable.getTableSchema())
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .skipInvalidRows()
        );

        pipeline.run();
    }
}
