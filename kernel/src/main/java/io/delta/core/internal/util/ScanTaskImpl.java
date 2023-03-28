package io.delta.core.internal.util;

import io.delta.core.ScanTask;
import io.delta.core.data.ColumnarBatch;
import io.delta.core.internal.actions.AddFile;
import io.delta.core.utils.CloseableIterator;

public class ScanTaskImpl implements ScanTask {

    public ScanTaskImpl(AddFile addFile) {
        System.out.println("Created ScanTaskImpl for AddFile " + addFile.getPath());
    }

    @Override
    public CloseableIterator<ColumnarBatch> getData() {
        return null;
    }
}
