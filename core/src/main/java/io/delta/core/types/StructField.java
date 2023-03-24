package io.delta.core.types;

public class StructField {
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
