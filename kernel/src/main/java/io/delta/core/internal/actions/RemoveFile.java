package io.delta.core.internal.actions;

import java.util.Optional;

import io.delta.core.data.Row;
import io.delta.core.types.StructType;

public class RemoveFile extends FileAction {

    public static RemoveFile fromRow(Row row) {
        if (row == null) return null;

        return new RemoveFile("asdfasdf", true);
    }

    public static final StructType READ_SCHEMA = new StructType();

    public RemoveFile(String path, boolean dataChange) {
        super(path, dataChange);
    }

    public Optional<String> getDeletionVectorUniqueId() {
        return null;
    }

    @Override
    public RemoveFile copyWithDataChange(boolean dataChange) {
         return new RemoveFile(path, dataChange);
    }
}
