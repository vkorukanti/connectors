package io.delta.core.internal;

import java.util.Optional;

import io.delta.core.Scan;
import io.delta.core.ScanBuilder;
import io.delta.core.expressions.Expression;
import io.delta.core.internal.actions.AddFile;
import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;

public class ScanBuilderImpl implements ScanBuilder {

    private final StructType snapshotSchema;
    private final StructType snapshotPartitionSchema;
    private final CloseableIterator<AddFile> filesIter;

    private StructType readSchema;
    private Optional<Expression> filter;

    public ScanBuilderImpl(
            StructType snapshotSchema,
            StructType snapshotPartitionSchema,
            CloseableIterator<AddFile> filesIter) {
        this.snapshotSchema = snapshotSchema;
        this.snapshotPartitionSchema = snapshotPartitionSchema;
        this.filesIter = filesIter;

        this.readSchema = snapshotSchema;
        this.filter = Optional.empty();
    }

    @Override
    public ScanBuilder withReadSchema(StructType readSchema) {
        // TODO: validate
        this.readSchema = readSchema;
        return this;
    }

    @Override
    public ScanBuilder withFilter(Expression expression) {
        this.filter = Optional.of(expression);
        return this;
    }

    @Override
    public Scan build() {
        return new ScanImpl(snapshotSchema, readSchema, snapshotPartitionSchema, filesIter, filter);
    }
}
