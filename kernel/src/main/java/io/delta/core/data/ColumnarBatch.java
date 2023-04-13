package io.delta.core.data;

import io.delta.core.utils.CloseableIterator;

public interface ColumnarBatch {
    /**
     * Return the {@link ColumnVector} for the given ordinal in the columnar batch. If the ordinal
     * is not valid throws error (TODO:
     * @param ordinal
     * @return
     */
    ColumnVector getColumnVector(int ordinal);

    /**
     * Number of records/rows in the columnar batch.
     * @return
     */
    int getSize();

    /**
     * Return a slice of the current batch.
     * @param start Starting record to include in the returned columnar batch
     * @param end Ending record (exclusive) to include in the returned columnar batch
     * @return
     */
    ColumnarBatch slice(int start, int end);

    /**
     * Get an interator to read the data row by rows
     * @return
     */
    default CloseableIterator<Row> getRows() {

        ColumnarBatch batch = this;

        return new CloseableIterator<Row>() {

            int rowId = 0;
            int maxRowId = getSize();

            @Override
            public boolean hasNext() {
                return rowId < maxRowId;
            }

            @Override
            public Row next() {
                Row row = new ColumnarBatchRow(batch, rowId);
                rowId += 1;
                return row;
            }

            @Override
            public void close() { }
        };
    }
}
