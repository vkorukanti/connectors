package io.delta.kernel.client;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.DefaultBooleanColumnVector;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class DefaultExpressionHandler
    implements ExpressionHandler
{
    @Override
    public ExpressionEvaluator getEvaluator(StructType batchSchema, Expression expression)
    {
        return new DefaultExpressionEvaluator(batchSchema, expression);
    }

    private static class DefaultExpressionEvaluator
        implements ExpressionEvaluator
    {
        private final StructType schema;
        private final Expression expression;

        private DefaultExpressionEvaluator(StructType schema, Expression expression)
        {
            this.schema = schema;
            this.expression = expression;
        }

        @Override
        public ColumnVector eval(ColumnarBatch input)
        {
            if (!expression.dataType().equals(BooleanType.INSTANCE)) {
                throw new UnsupportedOperationException("not yet supported");
            }

            List<Boolean> result = new ArrayList<>();
            input.getRows().forEachRemaining(row -> result.add((Boolean) expression.eval(row)));
            return new DefaultBooleanColumnVector(result);
        }

        @Override
        public void close()
                throws Exception
        {
            // nothing to close
        }
    }
}
