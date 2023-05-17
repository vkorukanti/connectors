package io.delta.kernel;

import io.delta.kernel.expressions.Expression;
import io.delta.kernel.types.StructType;

/**
 * Builder to construct {@link Scan} object.
 */
public interface ScanBuilder {

    /**
     * Apply the given predicate expression to prune any files
     * that do not contain data satisfying the given predicate.
     *
     * @param predicate an {@link Expression} which evaluates to boolean.
     * @return A {@link ScanBuilder} with predicate applied.
     *
     * @throws InvalidExpressionException if the filter is not valid.
     */
    ScanBuilder withPredicate(Expression predicate)
            throws InvalidExpressionException;

    /**
     * Apply the given <i>readSchema</i>.
     *
     * @param readSchema Subset of columns to read from the Delta table.
     * @return A {@link ScanBuilder} with projection pruning.
     */
    ScanBuilder withProject(StructType readSchema);

    /**
     * @return Build the {@link Scan instance}
     */
    Scan build();
}
