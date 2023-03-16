package io.delta.core.helpers;

import java.io.Serializable;
import java.util.Optional;
import java.util.TimeZone;

import io.delta.core.ColumnMappingMode;
import io.delta.core.data.ColumnarBatch;
import io.delta.core.expressions.Expression;
import io.delta.core.fs.FileStatus;
import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;

public interface ScanHelper extends Serializable {

    // Note: for production, will likely be a builder pattern to deal with all these optional params

    CloseableIterator<ColumnarBatch> readParquetDataFile(
        FileStatus fileStatus,
        StructType physicalReadSchema,
        TimeZone timeZone,
        Optional<ColumnMappingMode> columnMappingMode,
        Optional<Expression> skippingFilter,
        Optional<FileStatus> deletionVectorFileStatus
    );
}
