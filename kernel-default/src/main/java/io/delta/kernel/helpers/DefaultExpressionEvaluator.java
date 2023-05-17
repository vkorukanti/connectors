package io.delta.kernel.helpers;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.types.StructType;

public class DefaultExpressionEvaluator
    implements ExpressionEvaluator
{
    private final StructType schema;
    private final Expression expression;

    public DefaultExpressionEvaluator(StructType schema, Expression expression)
    {
        this.schema = schema;
        this.expression = expression;
    }

    @Override
    public ColumnVector eval(ColumnarBatch input)
    {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void close()
            throws Exception
    {
        // Nothing to close
    }
}
