package io.delta.kernel;

import io.delta.kernel.client.TableClient;
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
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @param predicate an {@link Expression} which evaluates to boolean.
     * @return A {@link ScanBuilder} with predicate applied.
     *
     * @throws InvalidExpressionException if the filter is not valid.
     */
    ScanBuilder withPredicate(TableClient tableClient, Expression predicate)
            throws InvalidExpressionException;

    /**
     * Apply the given <i>readSchema</i>.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @param readSchema Subset of columns to read from the Delta table.
     * @return A {@link ScanBuilder} with projection pruning.
     */
    ScanBuilder withProjects(TableClient tableClient, StructType readSchema);

    /**
     * @return Build the {@link Scan instance}
     */
    Scan build();
}
