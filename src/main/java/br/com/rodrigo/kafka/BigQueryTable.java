package br.com.rodrigo.kafka;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;


public class BigQueryTable implements Serializable {

    private static final long serialVersionUID = 1123L;

    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    private String datalake;
    private String dataset;
    private String tableName;
    private BigQuery bigquery;
    private Table table;

    public BigQueryTable(String datalake, String dataset, String tableName ){
        this.datalake = datalake;
        this.dataset = dataset;
        this.tableName = tableName;

        this.bigquery = BigQueryOptions.getDefaultInstance().getService();
        this.table = this.buildTable();
    }

    public String getTableName() {
        return String.format("%s.%s.%s", this.datalake, this.dataset, this.tableName);
    }

    public boolean tableExists() {
        return this.table != null && this.table.exists();
    }

    public TableReference getTableReference() {
        TableReference tableReference = new TableReference()
            .setProjectId(this.datalake)
            .setDatasetId(this.dataset)
            .setTableId(this.tableName);

        return tableReference;
    }

    public void refreshState() {
        this.bigquery = BigQueryOptions.getDefaultInstance().getService();
        this.table = this.buildTable();
    }

    private Table buildTable() {
        return this.bigquery.getTable(TableId.of(this.datalake, this.dataset, this.tableName));
    }

    public TableSchema getTableSchema() throws SchemaDefinitionNotFoundException {
        Schema tableSchema = this.table.getDefinition().getSchema();

        if (tableSchema == null) {
            throw new SchemaDefinitionNotFoundException(this.getTableName());
        }

        FieldList tableFields = tableSchema.getFields();

        List<TableFieldSchema> fieldSchemas = tableFields.stream()
            .map(this::convert)
            .collect(Collectors.toList());

        TableSchema schema = new TableSchema();
        schema.setFields(fieldSchemas);

        return schema;
    }

    private TableFieldSchema convert(Field source) {

        TableFieldSchema field = new TableFieldSchema();
        field.setName(source.getName());
        field.setDescription(source.getDescription());
        field.setMode(source.getMode().name());
        field.setType(source.getType().name());
        field.setPrecision(source.getPrecision());
        field.setScale(source.getScale());
        field.setMaxLength(source.getMaxLength());
        field.setDefaultValueExpression(source.getDefaultValueExpression());

        if (source.getSubFields() != null) {
            List<TableFieldSchema> fieldSchemas = source.getSubFields().stream()
                    .map(this::convert)
                    .collect(Collectors.toList());

            field.setFields(fieldSchemas);
        }
        return field;
    }
}
