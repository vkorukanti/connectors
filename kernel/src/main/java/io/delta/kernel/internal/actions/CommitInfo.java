package io.delta.kernel.internal.actions;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;

public class CommitInfo implements Action {

    public static CommitInfo fromRow(Row row) {
        if (row == null) return null;

        return new CommitInfo();
    }

    public static final StructType READ_SCHEMA = new StructType();
}
