package io.delta.core;

import io.delta.core.expressions.Expression;
import io.delta.core.types.StructType;
import io.delta.core.utils.Tuple2;

public interface ScanBuilder {

    /**
     * Apply the given predicate expression to prune any files
     * that do not contain data satisfying the given predicate.
     *
     * @param predicate an {@link Expression} which evaluates to boolean.
     * @return a tuple of ScanBuilder with filter applied and
     *         the remaining expression that can not be satisfied
     *         by the Delta Core and the connector needs to use
     *         the expression to filter out the data returned
     *         by Delta Core
     *
     * @throws InvalidExpressionException if the filter is not valid.
     */
    Tuple2<ScanBuilder, Expression> applyFilter(Expression predicate)
        throws InvalidExpressionException;

    /**
     * @return Build the {@link Scan instance}
     */
    Scan build();
}
