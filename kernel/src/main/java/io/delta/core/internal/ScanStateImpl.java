package io.delta.core.internal;

import io.delta.core.ScanState;
import io.delta.core.data.ColumnarBatch;
import io.delta.core.data.InputFile;
import io.delta.core.data.Row;
import io.delta.core.fs.Path;
import io.delta.core.helpers.ConnectorReadContext;
import io.delta.core.helpers.TableHelper;
import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;

import java.io.IOException;
import java.util.HashMap;

public class ScanStateImpl implements ScanState
{
    private final TableHelper tableHelper;

    public ScanStateImpl(TableHelper tableHelper) {
        this.tableHelper = tableHelper;
    }

    @Override
    public CloseableIterator<ColumnarBatch> getData(
            Row scanState,
            Row fileInfo,
            StructType readSchema,
            ConnectorReadContext connectorReadContext) throws IOException
    {
        return tableHelper.readParquetFile(
                connectorReadContext,
                new InputFile(fileInfo.getString(0)),
                readSchema, // TODO: may need column mapping processing
                new HashMap<>() // TODO: parse the partition values
        );
    }
}
