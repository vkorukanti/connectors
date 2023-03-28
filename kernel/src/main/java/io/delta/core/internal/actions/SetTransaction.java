package io.delta.core.internal.actions;

import io.delta.core.data.Row;
import io.delta.core.types.StructType;

public class SetTransaction implements Action {

    public static SetTransaction fromRow(Row row) {
        if (row == null) return null;

        return new SetTransaction();
    }

    public static final StructType READ_SCHEMA = new StructType();
}
