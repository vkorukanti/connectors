package io.delta.core.types;

import io.delta.core.data.Row;

public class StructField {

    ////////////////////////////////////////////////////////////////////////////////
    // Static Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    public static StructField fromRow(Row row) {
        final String name = row.getString(0);
        final DataType type = UnresolvedDataType.fromRow(row, 1);
        final boolean nullable = row.getBoolean(2);
        return new StructField(name, type, nullable);
    }

    public static final StructType READ_SCHEMA = new StructType()
        .add("name", StringType.INSTANCE)
        .add("type", UnresolvedDataType.INSTANCE)
        .add("nullable", BooleanType.INSTANCE);

    ////////////////////////////////////////////////////////////////////////////////
    // Instance Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    private final String name;
    private final DataType dataType;
    private final boolean nullable;
    // private final FieldMetadata metadata;

    public StructField(String name, DataType dataType, boolean nullable) {
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
    }

    public String getName() {
        return name;
    }

    public DataType getDataType() {
        return dataType;
    }

    public boolean isNullable() {
        return nullable;
    }

    @Override
    public String toString() {
        return String.format("StructField(name=%s,type=%s,nullable=%s)", name, dataType, nullable);
    }
}
