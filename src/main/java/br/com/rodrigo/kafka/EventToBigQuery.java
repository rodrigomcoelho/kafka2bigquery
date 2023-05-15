package br.com.rodrigo.kafka;


import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import br.com.rodrigo.kafka.override.BigQueryAvroUtil;


public class EventToBigQuery extends DoFn<GenericRecord, TableRow> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaToBigQuery.class);

    private final SerializableFunction<Void, BigQueryTable> bigQueryTableFn;
    private TableSchema tableSchema;

    public EventToBigQuery(SerializableFunction<Void, BigQueryTable> bigQueryTableFn) {
        this.bigQueryTableFn = bigQueryTableFn;
    }

    @Setup
    public void setup() throws SchemaDefinitionNotFoundException {
        tableSchema  =  bigQueryTableFn.apply(null).getTableSchema();
    }

    private TableRow convertAvroEventToTableRow(GenericRecord avroRecord) {
        return BigQueryAvroUtil.convertGenericRecordToTableRow(avroRecord, this.tableSchema);
    }

    private static void logRow(String row) {
        Date currentDate = new Date();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
        String formattedDate = dateFormat.format(currentDate);

        String messageFormated = String.format("[%s] %s", formattedDate, row);

        logger.debug(messageFormated);
    }

    @ProcessElement
    public void process(ProcessContext processContent) {
        GenericRecord event = processContent.element();

        TableRow row = convertAvroEventToTableRow(event);

        logRow(row.toString());

        processContent.output(row);
    }
}
