package io.delta.core.data;

import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class PartitionColumnWrapperColumnarBatch
    implements ColumnarBatch
{
    private StructType dataSchema;
    private StructType readSchema;
    private Map<String, String> partitionValues;
    private ColumnarBatch dataBatch;
    private Set<Integer> partitionColumnOrdinals;
    private Map<Integer, String> partitionColumnOrdinalToValue;
    private Map<Integer, Integer> dataColumnOrdinalToInnerBatchOrdinal;

    public PartitionColumnWrapperColumnarBatch(
            StructType dataSchema,
            StructType readSchema,
            Map<String, String> partitionValues,
            ColumnarBatch dataBatch)
    {
        this.dataSchema = requireNonNull(dataSchema, "dataSchema is not null");
        this.readSchema = requireNonNull(readSchema, "readSchema is not null");
        this.partitionValues = requireNonNull(partitionValues, "partitionValues is not null");
        this.dataBatch = requireNonNull(dataBatch, "dataBatch is not null");
    }

    @Override
    public ColumnVector getColumnVector(int ordinal)
    {
        if (partitionColumnOrdinals.contains(ordinal)) {
            return null; // TODO
        } else {
            return dataBatch.getColumnVector(ordinal);
        }
    }

    @Override
    public int getSize()
    {
        return dataBatch.getSize();
    }

    @Override
    public ColumnarBatch slice(int start, int end)
    {
        return null;
    }

    @Override
    public CloseableIterator<Row> getRows()
    {
        return ColumnarBatch.super.getRows();
    }
}
