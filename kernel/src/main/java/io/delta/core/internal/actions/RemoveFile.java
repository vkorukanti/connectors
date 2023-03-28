package io.delta.core.internal.actions;

import java.util.Optional;

public class RemoveFile implements FileAction {

    public Optional<String> getDeletionVectorUniqueId() {
        return null;
    }

    @Override
    public RemoveFile copyWithDataChange(boolean dataChange) {
        return null;
    }
}
