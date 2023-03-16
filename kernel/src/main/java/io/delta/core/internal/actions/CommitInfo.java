package io.delta.core.internal.actions;

import io.delta.core.data.Row;
import io.delta.core.types.StructType;

public class CommitInfo implements Action {

    public static CommitInfo fromRow(Row row) {
        if (row == null) return null;

        return new CommitInfo();
    }

    public static final StructType READ_SCHEMA = new StructType();
}
