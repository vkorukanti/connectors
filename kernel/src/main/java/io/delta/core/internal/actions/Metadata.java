package io.delta.core.internal.actions;

import io.delta.core.data.Row;
import io.delta.core.helpers.TableHelper;
import io.delta.core.types.*;

public class Metadata implements Action {

    public static Metadata fromRow(Row row, TableHelper tableHelper) {
        if (row == null) return null;

        final String schemaJson = row.getString(4);
        StructType schema = tableHelper.parseSchema(schemaJson);

        return new Metadata(schema);
    }

    /*
    {"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1679943453303}

     */
    public static final StructType READ_SCHEMA = new StructType()
        .add("id", StringType.INSTANCE)
        .add("name", StringType.INSTANCE)
        .add("description", StringType.INSTANCE)
        .add("format", Format.READ_SCHEMA)
        .add("schemaString", StringType.INSTANCE)
        .add("partitionColumns", new ArrayType(StringType.INSTANCE, false /* contains null */))
        .add("configuration", new MapType())
        .add("createdTime", LongType.INSTANCE);

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
