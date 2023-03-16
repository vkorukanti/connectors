package io.delta.core.internal;

import java.util.Optional;

import io.delta.core.InvalidExpressionException;
import io.delta.core.Scan;
import io.delta.core.ScanBuilder;
import io.delta.core.expressions.Expression;
import io.delta.core.fs.Path;
import io.delta.core.helpers.TableHelper;
import io.delta.core.internal.actions.AddFile;
import io.delta.core.internal.actions.Metadata;
import io.delta.core.internal.actions.Protocol;
import io.delta.core.internal.lang.Lazy;
import io.delta.core.utils.Tuple2;
import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;

public class ScanBuilderImpl implements ScanBuilder {

    private final StructType snapshotSchema;
    private final StructType snapshotPartitionSchema;
    private final CloseableIterator<AddFile> filesIter;
    private final Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata;
    private final TableHelper tableHelper;
    private final Path dataPath;

    private StructType readSchema;
    private Optional<Expression> filter;

    public ScanBuilderImpl(
            Path dataPath,
            Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata,
            StructType snapshotSchema,
            StructType snapshotPartitionSchema,
            CloseableIterator<AddFile> filesIter,
            TableHelper tableHelper) {
        this.dataPath = dataPath;
        this.snapshotSchema = snapshotSchema;
        this.snapshotPartitionSchema = snapshotPartitionSchema;
        this.filesIter = filesIter;
        this.protocolAndMetadata = protocolAndMetadata;
        this.tableHelper = tableHelper;

        this.readSchema = snapshotSchema;
        this.filter = Optional.empty();
    }

    @Override
    public Tuple2<ScanBuilder, Expression> applyFilter(Expression filter)
            throws InvalidExpressionException
    {
        // TODO: for now return the complete expression as the remaining expression.
        // Fix it later to return only non-partition column filter
        return new Tuple2<>(this, filter);
    }

    @Override
    public Scan build() {
        return new ScanImpl(
                snapshotSchema,
                readSchema,
                snapshotPartitionSchema,
                protocolAndMetadata,
                filesIter,
                filter,
                dataPath,
                tableHelper);
    }
}
