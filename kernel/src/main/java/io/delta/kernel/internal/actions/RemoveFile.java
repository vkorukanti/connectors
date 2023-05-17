package io.delta.kernel.internal.actions;

import java.util.Optional;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;

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
