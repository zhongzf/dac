using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace RaisingStudio.Data.Linq
{
    public static class ExpressionExtension
    {
        public static Expression<Func<T, bool>> Empty<T>()
        {
            return t => 0 == 0;
        }

        public static Expression OrderBy<T>(this Expression source, Expression<Func<T, object>> columnExpression)
        {
            if (source is LambdaExpression)
            {
                return Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T)), Expression.Quote(source), columnExpression);
            }
            else
            {
                return Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T)), source, columnExpression);
            }
        }

        public static Expression OrderBy(this Expression source, string column)
        {
            if (source is LambdaExpression)
            {
                return Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()), Expression.Quote(source), Expression.Constant(column));
            }
            else
            {
                return Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()), source, Expression.Constant(column));
            }
        }

        public static Expression OrderByDescending<T>(this Expression source, Expression<Func<T, object>> columnExpression)
        {
            if (source is LambdaExpression)
            {
                return Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T)), Expression.Quote(source), columnExpression);
            }
            else
            {
                return Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T)), source, columnExpression);
            }
        }

        public static Expression OrderByDescending(this Expression source, string column)
        {
            if (source is LambdaExpression)
            {
                return Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()), Expression.Quote(source), Expression.Constant(column));
            }
            else
            {
                return Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()), source, Expression.Constant(column));
            }
        }


        public static Expression Skip<T>(this Expression source, int count)
        {
            if (source is LambdaExpression)
            {
                return Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T)), Expression.Quote(source), Expression.Constant(count));
            }
            else
            {
                return Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T)), source, Expression.Constant(count));
            }
        }

        public static Expression Skip(this Expression source, int count)
        {
            if (source is LambdaExpression)
            {
                return Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()), Expression.Quote(source), Expression.Constant(count));
            }
            else
            {
                return Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()), source, Expression.Constant(count));
            }
        }

        public static Expression Take<T>(this Expression source, int count)
        {
            if (source is LambdaExpression)
            {
                return Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T)), Expression.Quote(source), Expression.Constant(count));
            }
            else
            {
                return Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()).MakeGenericMethod(typeof(T)), source, Expression.Constant(count));
            }
        }

        public static Expression Take(this Expression source, int count)
        {
            if (source is LambdaExpression)
            {
                return Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()), Expression.Quote(source), Expression.Constant(count));
            }
            else
            {
                return Expression.Call(null, ((MethodInfo)MethodBase.GetCurrentMethod()), source, Expression.Constant(count));
            }
        }
    }
}
