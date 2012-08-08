using System;
using System.Collections.Generic;
using System.Text;

using System.Linq;
using System.Linq.Expressions;

namespace RaisingStudio.Data.Linq
{
    public class ExpressionNominator : ExpressionVisitor
    {
        private Func<Expression, bool> canBeEvaluated;
        private HashSet<Expression> candidates;

        internal ExpressionNominator(Func<Expression, bool> canBeEvaluated)
        {
            this.canBeEvaluated = canBeEvaluated;
        }

        internal HashSet<Expression> Nominate(Expression expression)
        {
            this.candidates = new HashSet<Expression>();
            this.Visit(expression);
            return this.candidates;
        }

        private bool shouldBeNominated = true;

        public override Expression Visit(Expression expression)
        {
            if (expression != null)
            {
                bool preShouldBeNominated = this.shouldBeNominated;
                this.shouldBeNominated = true;

                base.Visit(expression);

                if (this.shouldBeNominated)
                {
                    if (this.canBeEvaluated(expression))
                    {
                        this.candidates.Add(expression);
                        this.shouldBeNominated = true;
                    }
                    else
                    {
                        this.shouldBeNominated = false;
                    }
                }

                this.shouldBeNominated &= preShouldBeNominated;
            }

            return expression;
        }
    }
}
