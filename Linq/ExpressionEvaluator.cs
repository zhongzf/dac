using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;

namespace RaisingStudio.Data.Linq
{
    public class ExpressionEvaluator : ExpressionVisitor
    {
        private static bool CanBeEvaluated(Expression expression)
        {
            return expression.NodeType != ExpressionType.Parameter;
        }

        private Func<Expression, bool> canBeEvaluated;
        private HashSet<Expression> candidates;

        public ExpressionEvaluator() : this(CanBeEvaluated)
        {
        }

        public ExpressionEvaluator(Func<Expression, bool> canBeEvaluated)
        {
            this.canBeEvaluated = canBeEvaluated;
        }

        public Expression Evaluate(Expression expression)
        {
            this.candidates = new ExpressionNominator(this.canBeEvaluated).Nominate(expression);

            return this.Visit(expression);
        }

        public override Expression Visit(Expression expression)
        {
            if (expression != null)
            {
                if (candidates.Count > 0)
                {
                    if (this.candidates.Contains(expression))
                    {
                        if (expression.NodeType != ExpressionType.Constant)
                        {
                            object value = (((Expression.Lambda(expression)).Compile()).DynamicInvoke(null));
                            return Expression.Constant(value, expression.Type);
                        }
                        return expression;
                    }
                }

                return base.Visit(expression);
            }
            return expression;
        }
    }
}
