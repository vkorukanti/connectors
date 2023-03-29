package io.delta.core.internal.actions;

import io.delta.core.data.Row;
import io.delta.core.types.StructType;

public class Format {
    public static Format fromRow(Row row) {
        if (row == null) return null;

        return new Format();
    }

    public static final StructType READ_SCHEMA = new StructType();
}
