package io.delta.core.internal.actions;

import io.delta.core.data.Row;
import io.delta.core.types.IntegerType;
import io.delta.core.types.StructType;

public class Protocol implements Action {

    public static Protocol fromRow(Row row) {
        if (row == null) return null;
        final int minReaderVersion = row.getInt(0);
        final int minWriterVersion = row.getInt(1);
        return new Protocol(minReaderVersion, minWriterVersion);
    }

    public static final StructType READ_SCHEMA = new StructType()
        .add("minReaderVersion", IntegerType.INSTANCE)
        .add("minWriterVersion", IntegerType.INSTANCE);

    private final int minReaderVersion;
    private final int minWriterVersion;

    public Protocol(int minReaderVersion, int minWriterVersion) {
        this.minReaderVersion = minReaderVersion;
        this.minWriterVersion = minWriterVersion;
    }

    public int getMinReaderVersion()
    {
        return minReaderVersion;
    }

    public int getMinWriterVersion()
    {
        return minWriterVersion;
    }

    @Override
    public String toString() {
        return String.format("Protocol(%s,%s)", minReaderVersion, minWriterVersion);
    }
}
