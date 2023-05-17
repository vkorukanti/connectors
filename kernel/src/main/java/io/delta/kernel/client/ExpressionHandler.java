package io.delta.kernel.client;

import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.types.StructType;

/**
 * Provides expression evaluation capability to Delta Kernel. Delta Kernel can use this client
 * to evaluate predicate on partition filters, fill up partition column values and any computation
 * on data using {@link Expression}s.
 */
public interface ExpressionHandler
{
    /**
     * Create an {@link ExpressionEvaluator} that can evaluate the given <i>expression</i> on
     * {@link io.delta.kernel.data.ColumnarBatch} of data with given <i>schema</i>.
     * @param schema Schema of the input data.
     * @param expression Expression to evaluate.
     * @return An {@link ExpressionEvaluator} instance bound to the given expression and schema.
     */
    ExpressionEvaluator getEvaluator(StructType schema, Expression expression);
}
