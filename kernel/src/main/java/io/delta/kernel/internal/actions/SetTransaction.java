package io.delta.kernel.internal.actions;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;

public class SetTransaction implements Action {

    public static SetTransaction fromRow(Row row) {
        if (row == null) return null;

        return new SetTransaction();
    }

    public static final StructType READ_SCHEMA = new StructType();
}
