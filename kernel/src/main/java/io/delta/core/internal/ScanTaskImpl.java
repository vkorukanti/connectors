package io.delta.core.internal;

import io.delta.core.ScanTask;
import io.delta.core.data.ColumnarBatch;
import io.delta.core.internal.actions.AddFile;
import io.delta.core.utils.CloseableIterator;

public class ScanTaskImpl implements ScanTask {

    private final AddFile addFile;

    public ScanTaskImpl(AddFile addFile) {
        this.addFile = addFile;
    }

    @Override
    public CloseableIterator<ColumnarBatch> getData() {
        return null;
    }

    /** Visible for testing */
    public AddFile getAddFile() {
        return this.addFile;
    }
}
