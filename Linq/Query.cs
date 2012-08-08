using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace RaisingStudio.Data.Linq
{
    public class Query<T> : IQueryProvider, IOrderedQueryable<T>, IOrderedQueryable, IQueryable<T>, IQueryable, IEnumerable<T>, IEnumerable, IListSource
    {
        public DataContext DataContext { get; private set; }
        private Expression expression;

        public Query(DataContext dataContext, Expression expression)
        {
            this.DataContext = dataContext;
            this.expression = expression;
        }

        public Query(DataContext dataContext)
        {
            this.DataContext = dataContext;            
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression");
            }
            if (!typeof(IQueryable<TElement>).IsAssignableFrom(expression.Type))
            {
                throw new ArgumentException("expression");
            }
            return new Query<TElement>(this.DataContext, expression);
        }

        public IQueryable CreateQuery(Expression expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression");
            }
            Type entityType = DataProvider.GetEntityType(expression);
            Type type = typeof(IQueryable<>).MakeGenericType(new Type[]
	        {
		        entityType
	        });
            if (!type.IsAssignableFrom(expression.Type))
            {
                throw new ArgumentException("expression");
            }
            Type genericType = typeof(Query<>).MakeGenericType(new Type[]
	        {
		        entityType
	        });
            return (IQueryable)Activator.CreateInstance(genericType, new object[]
	        {
		        this.DataContext, 
		        expression
	        });
        }

        public TResult Execute<TResult>(Expression expression)
        {
            object value = Execute(expression);
            value = Convert.ChangeType(value, typeof(TResult));
            return (TResult)value;
        }

        public object Execute(Expression expression)
        {
            return this.DataContext.Provider.Execute(expression);
        }

        public IEnumerator<T> GetEnumerator()
        {
            Type entityType = DataProvider.GetEntityType(expression);
            if (entityType == typeof(T))
            {
                return ((IEnumerable<T>)this.DataContext.Provider.Query(this.expression)).GetEnumerator();
            }
            else
            {
                IEnumerable enumerable = this.DataContext.Provider.Query(this.expression);
                UnaryExpression u = (expression as MethodCallExpression).Arguments[1] as UnaryExpression;
                MethodCallExpression m = MethodCallExpression.Call(null, (this.expression as MethodCallExpression).Method, enumerable.AsQueryable().Expression, u);
                IQueryable<T> queryable = enumerable.AsQueryable().Provider.CreateQuery<T>(m);
                return queryable.GetEnumerator();
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        public Type ElementType
        {
            get { throw new NotImplementedException(); }
        }

        public Expression Expression
        {
            get
            {
                if (this.expression != null)
                {
                    return this.expression;
                }
                else
                {
                    return Expression.Constant(this);
                }
            }
        }

        public IQueryProvider Provider
        {
            get
            {
                return this;
            }
        }

        public bool ContainsListCollection
        {
            get { throw new NotImplementedException(); }
        }

        public IList GetList()
        {
            throw new NotImplementedException();
        }        
    }
}
