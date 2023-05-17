package io.delta.kernel.internal.actions;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;

public class AddCDCFile implements Action {
    public static AddCDCFile fromRow(Row row) {
        if (row == null) return null;

        return new AddCDCFile();
    }

    public static final StructType READ_SCHEMA = new StructType();
}
