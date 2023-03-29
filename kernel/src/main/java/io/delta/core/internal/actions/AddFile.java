package io.delta.core.internal.actions;

import java.util.Optional;

import io.delta.core.data.Row;
import io.delta.core.types.StringType;
import io.delta.core.types.StructType;

public class AddFile extends FileAction {

    public static AddFile fromRow(Row row) {
        if (row == null) return null;

        return new AddFile(row.getString(0), true);
    }

    public static final StructType READ_SCHEMA = new StructType()
        .add("path", new StringType());

    public AddFile(String path, boolean dataChange) {
        super(path, dataChange);
    }

    public Optional<String> getDeletionVectorUniqueId() {
        return Optional.empty(); // TODO
    }

    @Override
    public AddFile copyWithDataChange(boolean dataChange) {
        return this; // TODO
    }
}
