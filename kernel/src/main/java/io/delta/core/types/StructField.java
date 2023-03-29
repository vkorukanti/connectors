package io.delta.core.types;

import io.delta.core.data.Row;

public class StructField {
    public static final StructType READ_SCHEMA = new StructType()
        .add("name", StringType.INSTANCE)
        .add("type", new UnresolvedDataType())
        .add("nullable", BooleanType.INSTANCE);

    public static StructField fromRow(Row row) {
        final String name = row.getString(0);
        final DataType type = UnresolvedDataType.fromRow(row.getRecord(1));
        final boolean nullable = true; // TODO row.getBoolean(2);
        return new StructField(name, type, nullable);
    }

    public final String name;
    public final DataType dataType;
    public final boolean nullable;
    // private final FieldMetadata metadata;

    public StructField(String name, DataType dataType, boolean nullable) {
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
    }
}
