package io.delta.core.internal.actions;

import java.util.Optional;

public class AddFile extends FileAction {
    public Optional<String> getDeletionVectorUniqueId() {
        return null;
    }


    @Override
    public AddFile copyWithDataChange(boolean dataChange) {
        return null;
    }
}
