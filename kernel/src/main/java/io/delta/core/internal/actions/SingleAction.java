package io.delta.core.internal.actions;

import io.delta.core.data.Row;
import io.delta.core.types.StructType;

public class SingleAction {

    public static SingleAction fromRow(Row row) {
        return null;
    }

    // TODO
    public static StructType READ_SCHEMA = new StructType();

    public Action unwrap() {
        return null;
    }
}
