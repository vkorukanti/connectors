package io.delta.core.internal.actions;

import io.delta.core.data.Row;
import io.delta.core.types.StructType;

public class Protocol implements Action {

    public static Protocol fromRow(Row row) {
        if (row == null) return null;

        return new Protocol();
    }

    public static final StructType READ_SCHEMA = new StructType();
}
