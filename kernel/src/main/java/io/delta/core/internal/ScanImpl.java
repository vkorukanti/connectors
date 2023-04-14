package io.delta.core.internal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.delta.core.Scan;
import io.delta.core.ScanTask;
import io.delta.core.data.ColumnVector;
import io.delta.core.data.ColumnarBatch;
import io.delta.core.expressions.Expression;
import io.delta.core.fs.Path;
import io.delta.core.helpers.TableHelper;
import io.delta.core.internal.actions.AddFile;
import io.delta.core.internal.actions.Metadata;
import io.delta.core.internal.actions.Protocol;
import io.delta.core.internal.data.AddFileColumnarBatch;
import io.delta.core.internal.data.MetadataProtocolColumnarBatch;
import io.delta.core.internal.data.PartitionRow;
import io.delta.core.internal.lang.FilteredCloseableIterator;
import io.delta.core.internal.lang.Lazy;
import io.delta.core.internal.lang.Tuple2;
import io.delta.core.internal.util.PartitionUtils;
import io.delta.core.types.BooleanType;
import io.delta.core.types.DataType;
import io.delta.core.types.LongType;
import io.delta.core.types.MapType;
import io.delta.core.types.StringType;
import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;

public class ScanImpl implements Scan {

    /** Complete schema of the snapshot to be scanned. */
    private final StructType snapshotSchema;

    private final Path dataPath;

    /** Schema that we actually want to read. */
    private final StructType readSchema;

    /** Partition schema. */
    private final StructType snapshotPartitionSchema;

    private final CloseableIterator<AddFile> filesIter;
    private final TableHelper tableHelper;

    /** Mapping from partitionColumnName to its ordinal in the `snapshotSchema`. */
    private final Map<String, Integer> partitionColumnOrdinals;

    private final Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata;

    private final Optional<Expression> metadataFilterConjunction;
    private final Optional<Expression> dataFilterConjunction;

    public ScanImpl(
            StructType snapshotSchema,
            StructType readSchema,
            StructType snapshotPartitionSchema,
            Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata,
            CloseableIterator<AddFile> filesIter,
            Optional<Expression> filter,
            Path dataPath,
            TableHelper tableHelper) {
        this.snapshotSchema = snapshotSchema;
        this.readSchema = readSchema;
        this.snapshotPartitionSchema = snapshotPartitionSchema;
        this.protocolAndMetadata = protocolAndMetadata;
        this.filesIter = filesIter;
        this.partitionColumnOrdinals = PartitionUtils.getPartitionOrdinals(snapshotSchema, snapshotPartitionSchema);
        this.dataPath = dataPath;
        this.tableHelper = tableHelper;

        if (filter.isPresent()) {
            final List<String> partitionColumns = snapshotPartitionSchema.fieldNames();
            final Tuple2<Optional<Expression>, Optional<Expression>> metadataAndDataConjunctions =
                PartitionUtils.splitMetadataAndDataPredicates(filter.get(), partitionColumns);

            this.metadataFilterConjunction = metadataAndDataConjunctions._1;
            this.dataFilterConjunction = metadataAndDataConjunctions._2;
        } else {
            this.metadataFilterConjunction = Optional.empty();
            this.dataFilterConjunction = Optional.empty();
        }

        System.out.println("ScanImpl: snapshotSchema: " + snapshotSchema.fields());
        System.out.println("ScanImpl: readSchema: " + readSchema.fields());
        System.out.println("ScanImpl: snapshotPartitionSchema: " + snapshotPartitionSchema.fields());
        System.out.println("ScanImpl: partitionColumnOrdinals: " + partitionColumnOrdinals.toString());

        System.out.println("ScanImpl: snapshotPartitionSchema: " + snapshotPartitionSchema.fields());
        System.out.println("ScanImpl: metadataFilterConjunction: " + metadataFilterConjunction.toString());
        System.out.println("ScanImpl: dataFilterConjunction: " + dataFilterConjunction.toString());
    }

    @Override
    public CloseableIterator<ScanTask> getTasks() {
        return new FilteredCloseableIterator<ScanTask, AddFile>(filesIter) {
            @Override
            protected Optional<ScanTask> accept(AddFile addFile) {
                if (!metadataFilterConjunction.isPresent()) {
                    return Optional.of(
                            new ScanTaskImpl(
                                    dataPath,
                                    addFile,
                                    protocolAndMetadata.get()._2.getConfiguration(),
                                    snapshotSchema,
                                    snapshotPartitionSchema,
                                    tableHelper
                            )
                    );
                }

                // Perform Partition Pruning
                final PartitionRow row = new PartitionRow(partitionColumnOrdinals, addFile.getPartitionValues());
                final boolean accept = (boolean) metadataFilterConjunction.get().eval(row);

                if (accept) {
                    return Optional.of(
                            new ScanTaskImpl(
                                    dataPath,
                                    addFile,
                                    protocolAndMetadata.get()._2.getConfiguration(),
                                    snapshotSchema,
                                    snapshotPartitionSchema,
                                    tableHelper
                            )
                    );
                }

                return Optional.empty();
            }
        };
    }

    /**
     * Get an iterator of data files in this version of scan that survived the predicate pruning.
     *
     * @return data in {@link ColumnarBatch} batch format. Each row correspond to one survived file.
     */
    public CloseableIterator<ColumnarBatch> survivedFileInfo() {
        return new CloseableIterator<ColumnarBatch>() {
            private Optional<AddFile> nextValid = Optional.empty();
            private boolean closed;

            @Override
            public boolean hasNext() {
                if (closed) {
                    throw new IllegalStateException("Can't call `hasNext` on a closed iterator.");
                }
                if (!nextValid.isPresent()) {
                    nextValid = findNextValid();
                }
                return nextValid.isPresent();
            }

            @Override
            public ColumnarBatch next() {
                if (closed) {
                    throw new IllegalStateException("Can't call `next` on a closed iterator.");
                }
                if (!hasNext()) throw new NoSuchElementException();

                List<AddFile> batchAddFiles = new ArrayList<>();
                do {
                    batchAddFiles.add(nextValid.get());
                } while (hasNext() && batchAddFiles.size() < 8);
                return new AddFileColumnarBatch(Collections.unmodifiableList(batchAddFiles));
            }

            @Override
            public void close() throws IOException {
                filesIter.close();
                this.closed = true;
            }

            private Optional<AddFile> findNextValid() {
                while (filesIter.hasNext()) {
                    final Optional<AddFile> acceptedElementOpt = accept(filesIter.next());
                    if (acceptedElementOpt.isPresent()) return acceptedElementOpt;
                }

                return Optional.empty();
            }

            private Optional<AddFile> accept(AddFile addFile) {
                if (!metadataFilterConjunction.isPresent()) {
                    return Optional.of(addFile);
                }

                // Perform Partition Pruning
                final PartitionRow row = new PartitionRow(partitionColumnOrdinals, addFile.getPartitionValues());
                if ((boolean) metadataFilterConjunction.get().eval(row)) {
                    return Optional.of(addFile);
                }

                return Optional.empty();
            }
        };
    }

    /**
     * Get the scan state associate with the current scan. This state is common to all survived
     * files.
     */
    public CloseableIterator<ColumnarBatch> getScanState() {
        return new CloseableIterator<ColumnarBatch>() {
            private final boolean accessed = false;
            @Override
            public void close() { }

            @Override
            public boolean hasNext()
            {
                return !accessed;
            }

            @Override
            public ColumnarBatch next()
            {
                return new MetadataProtocolColumnarBatch(
                        protocolAndMetadata.get()._2,
                        protocolAndMetadata.get()._1);
            }
        };
    }
}
