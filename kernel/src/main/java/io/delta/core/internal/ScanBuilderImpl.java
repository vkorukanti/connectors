package io.delta.core.internal;

import java.util.Map;
import java.util.Optional;

import io.delta.core.Scan;
import io.delta.core.ScanBuilder;
import io.delta.core.expressions.Expression;
import io.delta.core.fs.Path;
import io.delta.core.helpers.TableHelper;
import io.delta.core.internal.actions.AddFile;
import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;

public class ScanBuilderImpl implements ScanBuilder {

    private final StructType snapshotSchema;
    private final StructType snapshotPartitionSchema;
    private final CloseableIterator<AddFile> filesIter;
    private final Map<String, String> configuration;
    private final TableHelper tableHelper;
    private final Path dataPath;

    private StructType readSchema;
    private Optional<Expression> filter;

    public ScanBuilderImpl(
            Path dataPath,
            Map<String, String> configuration,
            StructType snapshotSchema,
            StructType snapshotPartitionSchema,
            CloseableIterator<AddFile> filesIter,
            TableHelper tableHelper) {
        this.dataPath = dataPath;
        this.snapshotSchema = snapshotSchema;
        this.snapshotPartitionSchema = snapshotPartitionSchema;
        this.filesIter = filesIter;
        this.configuration = configuration;
        this.tableHelper = tableHelper;

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
        return new ScanImpl(
                snapshotSchema,
                readSchema,
                snapshotPartitionSchema,
                configuration,
                filesIter,
                filter,
                dataPath,
                tableHelper);
    }
}
