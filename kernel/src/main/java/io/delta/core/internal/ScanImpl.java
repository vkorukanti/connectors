package io.delta.core.internal;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.delta.core.Scan;
import io.delta.core.ScanTask;
import io.delta.core.expressions.Expression;
import io.delta.core.fs.Path;
import io.delta.core.helpers.TableHelper;
import io.delta.core.internal.actions.AddFile;
import io.delta.core.internal.data.PartitionRow;
import io.delta.core.internal.lang.FilteredCloseableIterator;
import io.delta.core.internal.lang.Tuple2;
import io.delta.core.internal.util.PartitionUtils;
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

    private final Optional<Expression> metadataFilterConjunction;
    private final Optional<Expression> dataFilterConjunction;

    public ScanImpl(
            StructType snapshotSchema,
            StructType readSchema,
            StructType snapshotPartitionSchema,
            CloseableIterator<AddFile> filesIter,
            Optional<Expression> filter,
            Path dataPath,
            TableHelper tableHelper) {
        this.snapshotSchema = snapshotSchema;
        this.readSchema = readSchema;
        this.snapshotPartitionSchema = snapshotPartitionSchema;
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
                    return Optional.of(new ScanTaskImpl(dataPath, addFile, tableHelper));
                }

                // Perform Partition Pruning
                final PartitionRow row = new PartitionRow(partitionColumnOrdinals, addFile.getPartitionValues());
                final boolean accept = (boolean) metadataFilterConjunction.get().eval(row);

                if (accept) {
                    return Optional.of(new ScanTaskImpl(dataPath, addFile, tableHelper));
                }

                return Optional.empty();
            }
        };
    }
}
