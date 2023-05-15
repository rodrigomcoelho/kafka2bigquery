package br.com.rodrigo.kafka;

public class SchemaDefinitionNotFoundException  extends Exception{
    public SchemaDefinitionNotFoundException(String tableName) {
        super("Schema definition not found for table " + tableName);
    }
}
