package br.com.rodrigo.kafka;

import java.io.IOException;
import java.util.HashMap;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public class KafkaEventConfig {
    private String registryHost;
    private String registryUser;
    private String registerPassword;
    private String kafkaUser;
    private String kafkaPassword;
    private String kafkaTopic;
    private String consumerGroup;
    private String offset;

    public KafkaEventConfig(
        String registryHost,
        String registryUser,
        String registerPassword,
        String kafkaUser,
        String kafkaPassword,
        String kafkaTopic,
        String consumerGroup,
        String offset
    ) {
        this.kafkaTopic = kafkaTopic;
        this.registryHost = registryHost;
        this.registryUser = registryUser;
        this.registerPassword = registerPassword;
        this.kafkaUser = kafkaUser;
        this.kafkaPassword = kafkaPassword;
        this.consumerGroup = consumerGroup;
        this.offset = offset;
    }

    public SchemaMetadata getLatestKafkaSchema() throws IOException, RestClientException {

        RestService schemaRegistryRestService = new RestService(this.registryHost);

        CachedSchemaRegistryClient registryClient = new CachedSchemaRegistryClient(
            schemaRegistryRestService,
            10,
            this.getSchemaRegistryConfig()
        );

        String schemaRegistryTopicName = kafkaTopic + "-value";

        SchemaMetadata latestSchemaMetadata = registryClient.getLatestSchemaMetadata(schemaRegistryTopicName);

        return latestSchemaMetadata;
    }

    public HashMap<String, Object> getSchemaRegistryConfig() {
        String registryAuth = String.format("%s:%s", this.registryUser, this.registerPassword);

        HashMap<String, Object> schemaRegistryConfigs = new HashMap<>();
        schemaRegistryConfigs.put("basic.auth.credentials.source", "USER_INFO");
        schemaRegistryConfigs.put("basic.auth.user.info", registryAuth);
        schemaRegistryConfigs.put("schema.registry.url", this.registryHost);

        return schemaRegistryConfigs;
    }

    public HashMap<String, Object> getConsumerConfig() {
        String kafkaJaasConfig = String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
            this.kafkaUser,
            this.kafkaPassword
        );

        HashMap<String, Object> kafkaConfigs = new HashMap<>();
        kafkaConfigs.put("retry.backoff.ms", 500);
        kafkaConfigs.put("sasl.mechanism", "PLAIN");
        kafkaConfigs.put("sasl.jaas.config", kafkaJaasConfig);
        kafkaConfigs.put("security.protocol", "SASL_SSL");
        kafkaConfigs.put("ssl.endpoint.identification.algorithm", "https");
        kafkaConfigs.put("enable.auto.commit", "true");

        if (this.offset != null && !this.offset.isBlank())
            kafkaConfigs.put("auto.offset.reset", this.offset);

        kafkaConfigs.put("group.id", this.consumerGroup);

        return kafkaConfigs;
    }
}
