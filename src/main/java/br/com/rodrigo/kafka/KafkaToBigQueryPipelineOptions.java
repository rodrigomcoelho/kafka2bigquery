package br.com.rodrigo.kafka;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.options.Validation.Required;

public interface KafkaToBigQueryPipelineOptions extends DataflowPipelineOptions, SdkHarnessOptions{

    @Required()
    @Description("Kafka host")
    String getKafkaHost();
    void setKafkaHost(String kafkaHost);

    @Required()
    @Description("Kafka user")
    @Hidden()
    String getKafkaUser();
    void setKafkaUser(String kafkaUser);

    @Required()
    @Description("Kafka password")
    @Hidden()
    String getKafkaPassword();
    void setKafkaPassword(String kafkaPassword);

    @Required()
    @Description("Registry host")
    String getRegistryHost();
    void setRegistryHost(String registryHost);

    @Required()
    @Description("Registry user")
    @Hidden()
    String getRegistryUser();
    void setRegistryUser(String registryUser);

    @Required()
    @Description("Registry password")
    @Hidden()
    String getRegistryPassword();
    void setRegistryPassword(String registryPassword);

    @Required()
    @Description("Kafka Topic")
    String getKafkaTopic();
    void setKafkaTopic(String kafkaTopic);

    @Required()
    @Description("Kafka Consumer Group")
    @Default.String("teste2")
    String getConsumerGroup();
    void setConsumerGroup(String consumerGroup);

    @Required()
    @Description("Data Lake")
    String getDatalake();
    void setDatalake(String datalake);

    @Required()
    @Description("Dataset")
    String getDataset();
    void setDataset(String dataset);

    @Required()
    @Description("TableName")
    String getTableName();
    void setTableName(String tableName);

    @Description("Kafka Offset")
    @Default.String("earliest")
    String getOffset();
    void setOffset(String offset);

    @Description("Kafka Auto Commit")
    @Default.Boolean(true)
    boolean getAutoCommit();
    void setAutoCommit(boolean autoCommit);

    @Description("Write Mode: Replace/Append/Empty")
    @Default.String("Replace")
    String getWriteMode();
    void setWriteMode(String writeMode);

    @Description("Log Level: ALL, TRACE, DEBUG, INFO, WARN, ERROR, OFF")
    @Default.Enum(value="ERROR")
    String getLogLevel();
    void setLogLevel(String logLevel);
}
