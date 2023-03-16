package io.delta.core.internal.actions;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import io.delta.core.data.Row;
import io.delta.core.types.*;

public class AddFile extends FileAction {

    ////////////////////////////////////////////////////////////////////////////////
    // Static Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    public static AddFile fromRow(Row row) {
        if (row == null) return null;

        final String path = row.getString(0);
        final Map<String, String> partitionValues = row.getMap(1);
        final long size = row.getLong(2);
        final long modificationTime = row.getLong(3);
        final boolean dataChange = row.getBoolean(4);

        return new AddFile(path, partitionValues, size, modificationTime, dataChange);
    }

    public static final StructType READ_SCHEMA = new StructType()
        .add("path", StringType.INSTANCE)
        .add("partitionValues", new MapType(StringType.INSTANCE, StringType.INSTANCE, false))
        .add("size", LongType.INSTANCE)
        .add("modificationTime", LongType.INSTANCE)
        .add("dataChange", BooleanType.INSTANCE);

    ////////////////////////////////////////////////////////////////////////////////
    // Instance Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    private final Map<String, String> partitionValues;
    private final long size;
    private final long modificationTime;

    public AddFile(
            String path,
            Map<String, String> partitionValues,
            long size,
            long modificationTime,
            boolean dataChange) {
        super(path, dataChange);

        this.partitionValues = partitionValues;
        this.size = size;
        this.modificationTime = modificationTime;
    }

    public Optional<String> getDeletionVectorUniqueId() {
        return Optional.empty(); // TODO
    }

    @Override
    public AddFile copyWithDataChange(boolean dataChange) {
        return this; // TODO
    }

    public Map<String, String> getPartitionValues() {
        return Collections.unmodifiableMap(partitionValues);
    }

    public long getSize() {
        return size;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    @Override
    public String toString() {
        return "AddFile{" +
            "path='" + path + '\'' +
            ", partitionValues=" + partitionValues +
            ", size=" + size +
            ", modificationTime=" + modificationTime +
            ", dataChange=" + dataChange +
            '}';
    }
}
