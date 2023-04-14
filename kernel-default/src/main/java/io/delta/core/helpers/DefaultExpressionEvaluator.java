package io.delta.core.helpers;

import io.delta.core.data.ColumnVector;
import io.delta.core.data.ColumnarBatch;
import io.delta.core.expressions.Expression;
import io.delta.core.types.StructType;

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
