package io.delta.core.internal.actions;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.delta.core.data.Row;
import io.delta.core.helpers.TableHelper;
import io.delta.core.types.*;

public class Metadata implements Action {

    ////////////////////////////////////////////////////////////////////////////////
    // Static Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    public static Metadata fromRow(Row row, TableHelper tableHelper) {
        if (row == null) return null;
        final String id = row.getString(0);
        final String name = row.getString(1);
        final String description = row.getString(2);
        final Format format = Format.fromRow(row.getRecord(3));
        final Map<String, String> configuration = row.getMap(6);
        final String schemaJson = row.getString(4);
        final List<String> partitionColumns = row.getList(5);
        Row schemaRow = tableHelper.parseJson(schemaJson, StructType.READ_SCHEMA);
        StructType schema = StructType.fromRow(schemaRow);

        return new Metadata(schemaJson, schema, partitionColumns, configuration);
    }

    public static final StructType READ_SCHEMA = new StructType()
        .add("id", StringType.INSTANCE)
        .add("name", StringType.INSTANCE)
        .add("description", StringType.INSTANCE)
        .add("format", Format.READ_SCHEMA)
        .add("schemaString", StringType.INSTANCE)
        .add("partitionColumns", new ArrayType(StringType.INSTANCE, false /* contains null */))
        .add("configuration", new MapType(StringType.INSTANCE, StringType.INSTANCE, false))
        .add("createdTime", LongType.INSTANCE);

    ////////////////////////////////////////////////////////////////////////////////
    // Instance Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    // id
    // name
    // description
    // format
    // schemaString
    // partitionColumns
    // configuration
    // createdTime

    private final String schemaString; // TODO: Temporary
    private final StructType schema;
    private final List<String> partitionColumns;
    private final Map<String, String> configuration;

    public Metadata(
            String schemaString,
            StructType schema,
            List<String> partitionColumns,
            Map<String, String> configuration) {
        this.schemaString = schemaString;
        this.schema = schema;
        this.partitionColumns = partitionColumns;
        this.configuration = configuration;
    }

    public String getSchemaString()
    {
        return schemaString;
    }

    public StructType getSchema() {
        return schema;
    }

    public List<String> getPartitionColumns() {
        return partitionColumns;
    }

    public StructType getPartitionSchema() {
        return new StructType(
            partitionColumns.stream().map(schema::get).collect(Collectors.toList())
        );
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }
}
