package io.delta.kernel.internal;

import java.util.Optional;

import io.delta.kernel.InvalidExpressionException;
import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.fs.Path;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.utils.Tuple2;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

public class ScanBuilderImpl implements ScanBuilder
{

    private final StructType snapshotSchema;
    private final StructType snapshotPartitionSchema;
    private final CloseableIterator<AddFile> filesIter;
    private final Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata;
    private final TableClient tableClient;
    private final Path dataPath;

    private StructType readSchema;
    private Optional<Expression> filter;

    public ScanBuilderImpl(
            Path dataPath,
            Lazy<Tuple2<Protocol, Metadata>> protocolAndMetadata,
            StructType snapshotSchema,
            StructType snapshotPartitionSchema,
            CloseableIterator<AddFile> filesIter,
            TableClient tableClient) {
        this.dataPath = dataPath;
        this.snapshotSchema = snapshotSchema;
        this.snapshotPartitionSchema = snapshotPartitionSchema;
        this.filesIter = filesIter;
        this.protocolAndMetadata = protocolAndMetadata;
        this.tableClient = tableClient;

        this.readSchema = snapshotSchema;
        this.filter = Optional.empty();
    }

    @Override
    public ScanBuilder withPredicate(TableClient tableClient, Expression predicate)
            throws InvalidExpressionException
    {
        this.filter = Optional.of(predicate);
        return this;
    }

    @Override
    public ScanBuilder withProjects(TableClient tableClient, StructType readSchema)
    {
        // TODO: validate the readSchema is a subset of the table schema
        this.readSchema = readSchema;
        return this;
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
                tableClient);
    }
}
