package io.delta.core.internal.actions;

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
        final String schemaJson = row.getString(4);
        Row schemaRow = tableHelper.parseJson(schemaJson, StructType.READ_SCHEMA);
        StructType schema = StructType.fromRow(schemaRow);

        return new Metadata(schema);
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

    private final StructType schema;

    public Metadata(StructType schema) {
        this.schema = schema;
    }

    public StructType getSchema() {
        return schema;
    }
}
