package io.delta.core;

import io.delta.core.expressions.Expression;
import io.delta.core.types.StructType;

public interface ScanBuilder {

    /** For column pruning. */
    ScanBuilder withReadSchema(StructType schema);

    /** For partition pruning and filter push down. */
    ScanBuilder withFilter(Expression filter);

    Scan build();
}
