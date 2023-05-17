package io.delta.kernel.internal.actions;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

public class Format {
    public static Format fromRow(Row row) {
        if (row == null) return null;

        final String provider = row.getString(0);

        return new Format(provider);
    }

    public static final StructType READ_SCHEMA = new StructType()
        .add("provider", StringType.INSTANCE);
        // TODO: options

    private final String provider;

    public Format(String provider) {
        this.provider = provider;
    }
}
