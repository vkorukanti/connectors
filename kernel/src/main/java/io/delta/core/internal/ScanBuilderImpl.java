package io.delta.core.internal;

import io.delta.core.Scan;
import io.delta.core.ScanBuilder;
import io.delta.core.expressions.Expression;
import io.delta.core.internal.replay.LogReplay;
import io.delta.core.types.StructType;

public class ScanBuilderImpl implements ScanBuilder {

    private final LogReplay logReplay;

    public ScanBuilderImpl(LogReplay logReplay) {
        this.logReplay = logReplay;
    }

    @Override
    public ScanBuilder withReadSchema(StructType schema) {
        return null;
    }

    @Override
    public ScanBuilder withFilter(Expression filter) {
        return null;
    }

    @Override
    public Scan build() {
        return null;
    }
}
