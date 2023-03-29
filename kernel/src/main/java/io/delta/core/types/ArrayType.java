package io.delta.core.types;

public class ArrayType extends DataType {
    private final DataType elementType;
    private final boolean containsNull;

    public ArrayType(DataType elementType, boolean containsNull) {
        this.elementType = elementType;
        this.containsNull = containsNull;
    }
}
