package io.delta.core.internal.actions;

import io.delta.core.data.Row;
import io.delta.core.types.StructType;

public class AddCDCFile implements Action {
    public static AddCDCFile fromRow(Row row) {
        if (row == null) return null;

        return new AddCDCFile();
    }

    public static final StructType READ_SCHEMA = new StructType();
}
