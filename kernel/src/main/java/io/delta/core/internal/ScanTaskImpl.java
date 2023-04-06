package io.delta.core.internal;

import io.delta.core.ScanTask;
import io.delta.core.data.ColumnarBatch;
import io.delta.core.fs.Path;
import io.delta.core.helpers.TableHelper;
import io.delta.core.internal.actions.AddFile;
import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;

import java.io.IOException;

public class ScanTaskImpl implements ScanTask {

    private final Path dataPath;
    private final AddFile addFile;
    private final TableHelper tableHelper;

    public ScanTaskImpl(Path dataPath, AddFile addFile, TableHelper tableHelper) {
        this.dataPath = dataPath;
        this.addFile = addFile;
        this.tableHelper = tableHelper;
    }

    @Override
    public CloseableIterator<ColumnarBatch> getData(StructType readSchema) throws IOException
    {
        return tableHelper.readParquetFileAsBatches(
                new Path(dataPath, addFile.toPath()).toString(),
                readSchema);
    }

    /** Visible for testing */
    public AddFile getAddFile() {
        return this.addFile;
    }
}
