package br.com.rodrigo.kafka.override;


import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Verify.verify;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Verify.verifyNotNull;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

/*
 * Essa função foi alterada para permitir alguns tipos funconarem;
 * Originalmente o código da função
 * https://github.com/apache/beam/blob/master/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/bigquery/BigQueryAvroUtils.java
 */

public class BigQueryAvroUtil {
    static final ImmutableMultimap<String, Type> BIG_QUERY_TO_AVRO_TYPES = ImmutableMultimap.<String, Type>builder()
            .put("STRING", Type.STRING)
            .put("GEOGRAPHY", Type.STRING)
            .put("BYTES", Type.BYTES)
            .put("INTEGER", Type.INT)
            .put("INTEGER", Type.LONG)
            .put("INT64", Type.LONG)
            .put("FLOAT", Type.DOUBLE)
            .put("FLOAT", Type.FLOAT)
            .put("FLOAT64", Type.DOUBLE)
            .put("NUMERIC", Type.BYTES)
            .put("BIGNUMERIC", Type.BYTES)
            .put("BOOLEAN", Type.BOOLEAN)
            .put("BOOL", Type.BOOLEAN)
            .put("TIMESTAMP", Type.LONG)
            .put("RECORD", Type.RECORD)
            .put("STRUCT", Type.RECORD)
            .put("DATE", Type.STRING)
            .put("DATE", Type.INT)
            .put("DATETIME", Type.STRING)
            .put("TIME", Type.STRING)
            .put("TIME", Type.LONG)
            .put("JSON", Type.STRING)
            .build();

    private static final DateTimeFormatter DATE_AND_SECONDS_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
            .withZoneUTC();

    @VisibleForTesting
    static String formatTimestamp(Long timestampMicro) {
        // timestampMicro is in "microseconds since epoch" format,
        // e.g., 1452062291123456L means "2016-01-06 06:38:11.123456 UTC".
        // Separate into seconds and microseconds.
        long timestampSec = timestampMicro / 1_000_000;
        long micros = timestampMicro % 1_000_000;
        if (micros < 0) {
            micros += 1_000_000;
            timestampSec -= 1;
        }
        String dayAndTime = DATE_AND_SECONDS_FORMATTER.print(timestampSec * 1000);

        if (micros == 0) {
            return String.format("%s UTC", dayAndTime);
        }
        return String.format("%s.%06d UTC", dayAndTime, micros);
    }

    /**
     * This method formats a BigQuery DATE value into a String matching the format
     * used by JSON
     * export. Date records are stored in "days since epoch" format, and BigQuery
     * uses the proleptic
     * Gregorian calendar.
     */
    private static String formatDate(int date) {
        return LocalDate.ofEpochDay(date).format(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE);
    }

    private static final java.time.format.DateTimeFormatter ISO_LOCAL_TIME_FORMATTER_MICROS = new DateTimeFormatterBuilder()
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .appendLiteral('.')
            .appendFraction(NANO_OF_SECOND, 6, 6, false)
            .toFormatter();

    private static final java.time.format.DateTimeFormatter ISO_LOCAL_TIME_FORMATTER_MILLIS = new DateTimeFormatterBuilder()
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .appendLiteral('.')
            .appendFraction(NANO_OF_SECOND, 3, 3, false)
            .toFormatter();

    private static final java.time.format.DateTimeFormatter ISO_LOCAL_TIME_FORMATTER_SECONDS = new DateTimeFormatterBuilder()
            .appendValue(HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2)
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2)
            .toFormatter();

    /**
     * This method formats a BigQuery TIME value into a String matching the format
     * used by JSON
     * export. Time records are stored in "microseconds since midnight" format.
     */
    private static String formatTime(long timeMicros) {
        java.time.format.DateTimeFormatter formatter;
        if (timeMicros % 1000000 == 0) {
            formatter = ISO_LOCAL_TIME_FORMATTER_SECONDS;
        } else if (timeMicros % 1000 == 0) {
            formatter = ISO_LOCAL_TIME_FORMATTER_MILLIS;
        } else {
            formatter = ISO_LOCAL_TIME_FORMATTER_MICROS;
        }
        return LocalTime.ofNanoOfDay(timeMicros * 1000).format(formatter);
    }

    static TableSchema trimBigQueryTableSchema(TableSchema inputSchema, Schema avroSchema) {
        List<TableFieldSchema> subSchemas = inputSchema.getFields()
            .stream()
            .flatMap(fieldSchema -> mapTableFieldSchema(fieldSchema, avroSchema))
            .collect(Collectors.toList());

        return new TableSchema().setFields(subSchemas);
    }

    private static Stream<TableFieldSchema> mapTableFieldSchema(
        TableFieldSchema fieldSchema,
        Schema avroSchema
    ) {
        Field avroFieldSchema = avroSchema.getField(fieldSchema.getName());

        if (avroFieldSchema == null)
            return Stream.empty();

        if (avroFieldSchema.schema().getType() != Type.RECORD)
            return Stream.of(fieldSchema);


        List<TableFieldSchema> subSchemas = fieldSchema
            .getFields()
            .stream()
            .flatMap(subSchema -> mapTableFieldSchema(subSchema, avroFieldSchema.schema()))
            .collect(Collectors.toList()
        );

        TableFieldSchema output = new TableFieldSchema()
            .setCategories(fieldSchema.getCategories())
            .setDescription(fieldSchema.getDescription())
            .setFields(subSchemas)
            .setMode(fieldSchema.getMode())
            .setName(fieldSchema.getName())
            .setType(fieldSchema.getType()
        );

        return Stream.of(output);
    }

    public static TableRow convertGenericRecordToTableRow(GenericRecord record, TableSchema schema) {
        return convertGenericRecordToTableRow(record, schema.getFields());
    }

    private static TableRow convertGenericRecordToTableRow(
        GenericRecord record,
        List<TableFieldSchema> fields
    ) {
        TableRow row = new TableRow();
        for (TableFieldSchema subSchema : fields) {

            Field field = record.getSchema().getField(subSchema.getName());

            Object convertedValue = getTypedCellValue(field.schema(), subSchema, record.get(field.name()));

            if (convertedValue != null) {
                row.set(field.name(), convertedValue);
            }
        }

        return row;
    }

    private static @Nullable Object getTypedCellValue(
        Schema avroSchema,
        TableFieldSchema fieldSchema,
        Object value
    ) {

        String mode = firstNonNull(fieldSchema.getMode(), "NULLABLE");

        switch (mode) {
            case "REQUIRED":
                return convertRequiredField(avroSchema.getType(), avroSchema.getLogicalType(), fieldSchema, value);

            case "REPEATED":
                return convertRepeatedField(avroSchema, fieldSchema, value);

            case "NULLABLE":
                return convertNullableField(avroSchema, fieldSchema, value);

            default:
                throw new UnsupportedOperationException(
                    "Parsing a field with BigQuery field schema mode " + fieldSchema.getMode()
                );
        }
    }

    private static List<Object> convertRepeatedField(
            Schema schema, TableFieldSchema fieldSchema, Object v) {
        Type arrayType = schema.getType();
        verify(
                arrayType == Type.ARRAY,
                "BigQuery REPEATED field %s should be Avro ARRAY, not %s",
                fieldSchema.getName(),
                arrayType);
        // REPEATED fields are represented as Avro arrays.
        if (v == null) {
            // Handle the case of an empty repeated field.
            return new ArrayList<>();
        }
        @SuppressWarnings("unchecked")
        List<Object> elements = (List<Object>) v;
        ArrayList<Object> values = new ArrayList<>();
        Type elementType = schema.getElementType().getType();
        LogicalType elementLogicalType = schema.getElementType().getLogicalType();
        for (Object element : elements) {
            values.add(convertRequiredField(elementType, elementLogicalType, fieldSchema, element));
        }
        return values;
    }

    private static Object convertRequiredField(
        Type avroType,
        LogicalType avroLogicalType,
        TableFieldSchema fieldSchema,
        Object value
    ) {

        checkNotNull(value, "REQUIRED field %s should not be null", fieldSchema.getName());

        String bqType = fieldSchema.getType();

        ImmutableCollection<Type> expectedAvroTypes = BIG_QUERY_TO_AVRO_TYPES.get(bqType);

        verifyNotNull(expectedAvroTypes, "Unsupported BigQuery type: %s", bqType);
        verify(
            expectedAvroTypes.contains(avroType),
            "Expected Avro schema types %s for BigQuery %s field %s, but received %s",
            expectedAvroTypes,
            bqType,
            fieldSchema.getName(),
            avroType
        );

        switch (bqType) {
            case "STRING":
            case "DATETIME":
            case "GEOGRAPHY":
            case "JSON":

                verify(value instanceof CharSequence, "Expected CharSequence (String), got %s", value.getClass());

                return value.toString();

            case "DATE":

                if (avroType == Type.INT) {
                    verify(value instanceof Integer, "Expected Integer, got %s", value.getClass());
                    verifyNotNull(avroLogicalType, "Expected Date logical type");
                    verify(avroLogicalType instanceof LogicalTypes.Date, "Expected Date logical type");

                    return formatDate((Integer) value);
                }

                verify(value instanceof CharSequence, "Expected CharSequence (String), got %s", value.getClass());

                return value.toString();

            case "TIME":

                if (avroType == Type.LONG) {
                    verify(value instanceof Long, "Expected Long, got %s", value.getClass());
                    verifyNotNull(avroLogicalType, "Expected TimeMicros logical type");
                    verify(
                        avroLogicalType instanceof LogicalTypes.TimeMicros,
                        "Expected TimeMicros logical type"
                    );

                    return formatTime((Long) value);
                }

                verify(value instanceof CharSequence, "Expected CharSequence (String), got %s", value.getClass());
                return value.toString();

            case "INTEGER":
            case "INT64":

                if (avroType == Type.INT) {
                    verify(value instanceof Integer, "Expected Integer, got %s", value.getClass());

                    return value.toString();
                }

                verify(value instanceof Long, "Expected Long, got %s", value.getClass());
                return ((Long) value).toString();

            case "FLOAT":
            case "FLOAT64":

                if (avroType == Type.FLOAT) {
                    verify(value instanceof Float, "Expected Double, got %s", value.getClass());
                    return value;
                }

                verify(value instanceof Double, "Expected Double, got %s", value.getClass());
                return value;

            case "NUMERIC":
            case "BIGNUMERIC":

                verify(value instanceof ByteBuffer, "Expected ByteBuffer, got %s", value.getClass());
                verifyNotNull(avroLogicalType, "Expected Decimal logical type");
                verify(avroLogicalType instanceof LogicalTypes.Decimal, "Expected Decimal logical type");

                BigDecimal numericValue = new Conversions.DecimalConversion().fromBytes((ByteBuffer) value, Schema.create(avroType), avroLogicalType);

                return numericValue.toString();

            case "BOOL":
            case "BOOLEAN":

                verify(value instanceof Boolean, "Expected Boolean, got %s", value.getClass());
                return value;

            case "TIMESTAMP":
                // TIMESTAMP data types are represented as Avro LONG types, microseconds since
                // the epoch.
                // Values may be negative since BigQuery timestamps start at 0001-01-01 00:00:00
                // UTC.
                verify(value instanceof Long, "Expected Long, got %s", value.getClass());

                return formatTimestamp((Long) value);

            case "RECORD":
            case "STRUCT":

                verify(value instanceof GenericRecord, "Expected GenericRecord, got %s", value.getClass());

                return convertGenericRecordToTableRow((GenericRecord) value, fieldSchema.getFields());

            case "BYTES":

                verify(value instanceof ByteBuffer, "Expected ByteBuffer, got %s", value.getClass());

                ByteBuffer byteBuffer = (ByteBuffer) value;
                byte[] bytes = new byte[byteBuffer.limit()];
                byteBuffer.get(bytes);

                return BaseEncoding.base64().encode(bytes);

            default:
                throw new UnsupportedOperationException(
                    String.format(
                        "Unexpected BigQuery field schema type %s for field named %s",
                        fieldSchema.getType(),
                        fieldSchema.getName()
                    )
                );
        }
    }

    private static @Nullable Object convertNullableField(
        Schema avroSchema,
        TableFieldSchema fieldSchema,
        Object value
    ) {
        // NULLABLE fields are represented as an Avro Union of the corresponding type
        // and "null".
        verify(
            avroSchema.getType() == Type.UNION,
            "Expected Avro schema type UNION, not %s, for BigQuery NULLABLE field %s",
            avroSchema.getType(),
            fieldSchema.getName()
        );

        List<Schema> unionTypes = avroSchema.getTypes();

        verify(
            unionTypes.size() == 2,
            "BigQuery NULLABLE field %s should be an Avro UNION of NULL and another type, not %s",
            fieldSchema.getName(),
            unionTypes
        );

        if (value == null)
            return null;

        Type firstType = unionTypes.get(0).getType();

        if (!firstType.equals(Type.NULL)) {
            return convertRequiredField(firstType, unionTypes.get(0).getLogicalType(), fieldSchema, value);
        }

        return convertRequiredField(
                unionTypes.get(1).getType(),
                unionTypes.get(1).getLogicalType(),
                fieldSchema,
                value
            );
    }

    static Schema toGenericAvroSchema(String schemaName, List<TableFieldSchema> fieldSchemas) {
        List<Field> avroFields = new ArrayList<>();

        for (TableFieldSchema bigQueryField : fieldSchemas) {
            avroFields.add(convertField(bigQueryField));
        }

        return Schema.createRecord(
            schemaName,
            "Translated Avro Schema for " + schemaName,
            "org.apache.beam.sdk.io.gcp.bigquery",
            false,
            avroFields
        );
    }

    // Avro library not annotated
    @SuppressWarnings({ "nullness" })
    private static Field convertField(TableFieldSchema bigQueryField) {
        ImmutableCollection<Type> avroTypes = BIG_QUERY_TO_AVRO_TYPES.get(bigQueryField.getType());

        if (avroTypes.isEmpty()) {
            throw new IllegalArgumentException(
                "Unable to map BigQuery field type " +
                bigQueryField.getType()
                + " to avro type."
            );
        }

        Type avroType = avroTypes.iterator().next();
        Schema elementSchema;

        if (avroType == Type.RECORD) {
            elementSchema = toGenericAvroSchema(bigQueryField.getName(), bigQueryField.getFields());
        } else {
            elementSchema = handleAvroLogicalTypes(bigQueryField, avroType);
        }

        Schema fieldSchema;

        if (bigQueryField.getMode() == null || "NULLABLE".equals(bigQueryField.getMode())) {
            fieldSchema = Schema.createUnion(Schema.create(Type.NULL), elementSchema);
        } else if ("REQUIRED".equals(bigQueryField.getMode())) {
            fieldSchema = elementSchema;
        } else if ("REPEATED".equals(bigQueryField.getMode())) {
            fieldSchema = Schema.createArray(elementSchema);
        } else {
            throw new IllegalArgumentException(
                String.format("Unknown BigQuery Field Mode: %s", bigQueryField.getMode())
            );
        }

        return new Field(
            bigQueryField.getName(),
            fieldSchema,
            bigQueryField.getDescription(),
            (Object) null /* Cast to avoid deprecated JsonNode constructor. */
        );
    }

    private static Schema handleAvroLogicalTypes(TableFieldSchema bigQueryField, Type avroType) {
        String bqType = bigQueryField.getType();

        switch (bqType) {
            case "NUMERIC":
                // Default value based on
                // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
                int precision = Optional.ofNullable(bigQueryField.getPrecision()).orElse(38L).intValue();
                int scale = Optional.ofNullable(bigQueryField.getScale()).orElse(9L).intValue();

                return LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Type.BYTES));

            case "BIGNUMERIC":
                // Default value based on
                // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
                int precisionBigNumeric = Optional.ofNullable(bigQueryField.getPrecision()).orElse(77L).intValue();
                int scaleBigNumeric = Optional.ofNullable(bigQueryField.getScale()).orElse(38L).intValue();

                return LogicalTypes.decimal(precisionBigNumeric, scaleBigNumeric)
                        .addToSchema(Schema.create(Type.BYTES));

            case "TIMESTAMP":

                return LogicalTypes.timestampMicros().addToSchema(Schema.create(Type.LONG));

            case "GEOGRAPHY":

                Schema geoSchema = Schema.create(Type.STRING);
                geoSchema.addProp(LogicalType.LOGICAL_TYPE_PROP, "geography_wkt");
                return geoSchema;

            default:
                return Schema.create(avroType);
        }
    }
}
