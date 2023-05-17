package io.delta.kernel.internal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import io.delta.kernel.Scan;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.fs.Path;
import io.delta.kernel.helpers.TableHelper;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.data.AddFileColumnarBatch;
import io.delta.kernel.internal.data.ScanStateColumnarBatch;
import io.delta.kernel.internal.data.PartitionRow;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.utils.Tuple2;
import io.delta.kernel.internal.util.PartitionUtils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

public class ScanImpl implements Scan
{

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

    /**
     * Get an iterator of data files in this version of scan that survived the predicate pruning.
     *
     * @return data in {@link ColumnarBatch} batch format. Each row correspond to one survived file.
     */
    public CloseableIterator<ColumnarBatch> getScanFiles() {
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
                    nextValid = Optional.empty();
                } while (batchAddFiles.size() < 8 && hasNext());
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
    public Row getScanState() {
        return new ScanStateColumnarBatch(
            protocolAndMetadata.get()._2,
            protocolAndMetadata.get()._1,
            dataPath.toUri().toString()).getRows().next();
    }
}
