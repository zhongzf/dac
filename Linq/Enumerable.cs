using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace RaisingStudio.Data.Linq
{
    public static class Enumerable
    {
        public static IList ToList(this IEnumerable source)
        {
            IList list = null;
            int index = 0;
            foreach (var item in source)
            {
                if (index == 0)
                {
                    Type entityType = item.GetType();
                    Type listType = typeof(List<>);
                    System.Type dataObjectListType = listType.MakeGenericType(entityType);
                    list = System.Activator.CreateInstance(dataObjectListType) as IList;
                }
                list.Add(item);
                index++;
            }
            return list;
        }

        public static object[] ToArray(this IEnumerable source)
        {
            int capacity = 4;
            object[] values = null;
            Type entityType = null;
            int index = 0;
            foreach (var item in source)
            {
                if (index == 0)
                {
                    entityType = item.GetType();
                    values = Array.CreateInstance(entityType, capacity) as object[];
                }
                if (index >= capacity)
                {
                    capacity = index * 2;
                    object[] array = Array.CreateInstance(entityType, capacity) as object[];
                    Array.Copy(values, 0, array, 0, index);
                    values = array;
                }
                values[index] = item;
                index++;
            }
            object[] result = Array.CreateInstance(entityType, index) as object[];
            Array.Copy(values, 0, result, 0, index);
            return result;
        }

        public static DataTable ToDataTable(this IEnumerable source)
        {
            if (source != null)
            {
                DataTable dataTable = new DataTable();
                string[] propertyNames;
                Type[] propertyTypes;
                Func<object, object>[] propertyGetters = null;
                int index = 0;
                foreach (var item in source)
                {
                    if (index == 0)
                    {
                        Type entityType = item.GetType();
                        var properties = entityType.GetProperties();
                        int propertyCount = properties.Length;
                        propertyNames = new string[propertyCount];
                        propertyTypes = new Type[propertyCount];
                        propertyGetters = new Func<object, object>[propertyCount];
                        for (int i = 0; i < propertyCount; i++)
                        {
                            var propertyInfo = properties[i];
                            string propertyName = propertyInfo.Name;
                            Type propertyType = propertyInfo.PropertyType;
                            propertyNames[i] = propertyName;
                            propertyTypes[i] = propertyType;

                            if ((propertyType.IsGenericType) && (propertyType.GetGenericTypeDefinition() == typeof(Nullable<>)))
                            {
                                Type argumentsType = propertyType.GetGenericArguments()[0];
                                var dataColumn = dataTable.Columns.Add(propertyName, argumentsType);
                                dataColumn.AllowDBNull = true;
                            }
                            else
                            {
                                dataTable.Columns.Add(propertyName, propertyType);
                            }


                            var instance = Expression.Parameter(typeof(object), "instance");
                            var call = Expression.Call(Expression.Convert(instance, entityType), propertyInfo.GetGetMethod());
                            var convert = Expression.Convert(call, typeof(object));
                            var expression = Expression.Lambda<Func<object, object>>(convert, instance);
                            var func = expression.Compile();
                            propertyGetters[i] = func;
                        }
                    }
                    DataRow dataRow = dataTable.NewRow();
                    object[] propertyValues = new object[propertyGetters.Length];
                    for (int i = 0; i < propertyGetters.Length; i++)
                    {
                        object propertyValue = propertyGetters[i](item);
                        propertyValues[i] = propertyValue;
                    }
                    dataRow.ItemArray = propertyValues;
                    dataTable.Rows.Add(dataRow);
                    index++;
                }
                dataTable.AcceptChanges();
                return dataTable;
            }
            return null;
        }


        public static IEnumerable ToEnumerable(this DataTable dataTable)
        {
            if (dataTable != null)
            {
                int columnCount = dataTable.Columns.Count;
                string[] propertyNames = new string[columnCount];
                Type[] propertyTypes = new Type[columnCount];
                Action<object, object>[] propertySetters = new Action<object, object>[columnCount];
                for (int i = 0; i < columnCount; i++)
                {
                    DataColumn dataColumn = dataTable.Columns[i];
                    propertyNames[i] = dataColumn.ColumnName;
                    Type dataType = dataColumn.DataType;
                    if ((dataType != typeof(string)) && (dataType != typeof(byte[])) && dataColumn.AllowDBNull)
                    {
                        Type nullableType = typeof(Nullable<>);
                        propertyTypes[i] = nullableType.MakeGenericType(dataType);
                    }
                    else
                    {
                        propertyTypes[i] = dataType;
                    }
                }
                Type entityType = TypeManager.CreateDynamicType("_", "<>_DynamicType", null, propertyNames, propertyTypes);
                Func<object> instanceCreater = Expression.Lambda<Func<object>>(Expression.New(entityType)).Compile();
                for (int i = 0; i < columnCount; i++)
                {
                    var instance = Expression.Parameter(typeof(object), "instance");
                    var value = Expression.Parameter(typeof(object), "value");
                    var convert = Expression.Convert(value, propertyTypes[i]);
                    var call = Expression.Call(Expression.Convert(instance, entityType), entityType.GetProperty(propertyNames[i]).GetSetMethod(), convert);
                    var expression = Expression.Lambda<Action<object, object>>(call, instance, value);
                    var action = expression.Compile();
                    propertySetters[i] = action;
                }
                foreach (DataRow dataRow in dataTable.Rows)
                {
                    object dataObject = instanceCreater();
                    object[] propertyValues = dataRow.ItemArray;
                    for (int i = 0; i < propertySetters.Length; i++)
                    {
                        object propertyValue = propertyValues[i];
                        if (Convert.IsDBNull(propertyValue))
                        {
                            propertyValue = null;
                        }
                        propertySetters[i](dataObject, propertyValue);
                    }
                    yield return dataObject;
                }
            }
        }

        public static IEnumerable<T> ToEnumerable<T>(this DataTable dataTable) where T : new()
        {
            if (dataTable != null)
            {
                int columnCount = dataTable.Columns.Count;
                string[] propertyNames = new string[columnCount];
                Type[] propertyTypes = new Type[columnCount];
                Action<T, object>[] propertySetters = new Action<T, object>[columnCount];
                for (int i = 0; i < columnCount; i++)
                {
                    DataColumn dataColumn = dataTable.Columns[i];
                    propertyNames[i] = dataColumn.ColumnName;
                    propertyTypes[i] = dataColumn.DataType;
                }
                Type entityType = typeof(T);
                for (int i = 0; i < columnCount; i++)
                {
                    PropertyInfo propertyInfo = entityType.GetProperty(propertyNames[i]);
                    if (propertyInfo != null)
                    {
                        var instance = Expression.Parameter(entityType, "instance");
                        var value = Expression.Parameter(typeof(object), "value");
                        var convert = Expression.Convert(value, propertyTypes[i]);
                        var call = Expression.Call(instance, propertyInfo.GetSetMethod(), convert);
                        var expression = Expression.Lambda<Action<T, object>>(call, instance, value);
                        var action = expression.Compile();
                        propertySetters[i] = action;
                    }
                }
                foreach (DataRow dataRow in dataTable.Rows)
                {
                    T dataObject = new T();
                    object[] propertyValues = dataRow.ItemArray;
                    for (int i = 0; i < propertySetters.Length; i++)
                    {
                        if (propertySetters[i] != null)
                        {
                            propertySetters[i](dataObject, propertyValues[i]);
                        }
                    }
                    yield return dataObject;
                }
            }
        }


        public static IEnumerable<T> ToEnumerable<T>(this IEnumerable source) where T : new()
        {
            if (source != null)
            {
                List<Action<object, T>> propertyTransferList = new List<Action<object, T>>();
                int index = 0;
                foreach (var item in source)
                {
                    if (index == 0)
                    {
                        Type entityType = item.GetType();
                        Type targetDataType = typeof(T);
                        var properties = entityType.GetProperties();
                        int propertyCount = properties.Length;
                        for (int i = 0; i < propertyCount; i++)
                        {
                            var propertyInfo = properties[i];
                            string propertyName = propertyInfo.Name;
                            var targetPropertyInfo = targetDataType.GetProperty(propertyName);
                            if (targetPropertyInfo != null)
                            {
                                var instance = Expression.Parameter(typeof(object), "instance");
                                var target = Expression.Parameter(targetDataType, "target");
                                var call = Expression.Call(Expression.Convert(instance, entityType), propertyInfo.GetGetMethod());
                                var target_call = Expression.Call(target, targetPropertyInfo.GetSetMethod(), call);
                                var expression = Expression.Lambda<Action<object, T>>(target_call, instance, target);
                                var action = expression.Compile();
                                propertyTransferList.Add(action);
                            }
                        }
                    }
                    T dataObject = new T();
                    foreach (var propertyTransfer in propertyTransferList)
                    {
                        propertyTransfer(item, dataObject);
                    }
                    yield return dataObject;
                    index++;
                }
            }
        }

        public static IEnumerable ToEnumerable(this IEnumerable source)
        {
            if (source != null)
            {
                PropertyInfo[] properties;
                string[] propertyNames;
                Type[] propertyTypes;
                Func<object> instanceCreater = null;
                Action<object, object>[] propertyTransfers = null;
                int index = 0;
                foreach (var item in source)
                {
                    if (index == 0)
                    {
                        Type entityType = item.GetType();
                        properties = entityType.GetProperties();
                        int propertyCount = properties.Length;
                        propertyNames = new string[propertyCount];
                        propertyTypes = new Type[propertyCount];
                        for (int i = 0; i < propertyCount; i++)
                        {
                            var propertyInfo = properties[i];
                            string propertyName = propertyInfo.Name;
                            Type propertyType = propertyInfo.PropertyType;
                            propertyNames[i] = propertyName;
                            propertyTypes[i] = propertyType;
                        }
                        Type targetDataType = TypeManager.CreateDynamicType("_", "<>_DynamicType", null, propertyNames, propertyTypes);
                        instanceCreater = Expression.Lambda<Func<object>>(Expression.New(targetDataType)).Compile();
                        propertyTransfers = new Action<object, object>[propertyCount];
                        for (int i = 0; i < propertyCount; i++)  
                        {
                            var propertyInfo = properties[i];
                            string propertyName = propertyInfo.Name;
                            var targetPropertyInfo = targetDataType.GetProperty(propertyName);
                            if (targetPropertyInfo != null)
                            {
                                var instance = Expression.Parameter(typeof(object), "instance");
                                var target = Expression.Parameter(typeof(object), "target");
                                var call = Expression.Call(Expression.Convert(instance, entityType), propertyInfo.GetGetMethod());
                                var target_call = Expression.Call(Expression.Convert(target, targetDataType), targetPropertyInfo.GetSetMethod(), call);
                                var expression = Expression.Lambda<Action<object, object>>(target_call, instance, target);
                                var action = expression.Compile();
                                propertyTransfers[i] = action;
                            }
                        }
                    }
                    object dataObject = instanceCreater();
                    foreach (var propertyTransfer in propertyTransfers)
                    {
                        propertyTransfer(item, dataObject);
                    }
                    yield return dataObject;
                    index++;
                }
            }
        }


        private static Func<object, object> GeneratePropertyGetter(Type entityType, PropertyInfo propertyInfo)
        {
            var instance = Expression.Parameter(typeof(object), "instance");
            var call = Expression.Call(Expression.Convert(instance, entityType), propertyInfo.GetGetMethod());
            var convert = Expression.Convert(call, typeof(object));
            var expression = Expression.Lambda<Func<object, object>>(convert, instance);
            var func = expression.Compile();
            return func;
        }

        public static IEnumerable ToItemArray(this IEnumerable source)
        {
            if (source != null)
            {
                string[] propertyNames;
                Type[] propertyTypes;
                Func<object, object>[] propertyGetters = null;
                int index = 0;
                foreach (var item in source)
                {
                    if (index == 0)
                    {
                        Type entityType = item.GetType();
                        var properties = entityType.GetProperties();
                        int propertyCount = properties.Length;
                        propertyNames = new string[propertyCount];
                        propertyTypes = new Type[propertyCount];
                        propertyGetters = new Func<object, object>[propertyCount];
                        for (int i = 0; i < propertyCount; i++)
                        {
                            var propertyInfo = properties[i];
                            string propertyName = propertyInfo.Name;
                            propertyNames[i] = propertyName;

                            Type propertyType = propertyInfo.PropertyType;
                            propertyTypes[i] = propertyType;

                            var func = GeneratePropertyGetter(entityType, propertyInfo);
                            propertyGetters[i] = func;
                        }
                    }
                    object[] propertyValues = new object[propertyGetters.Length];
                    for (int i = 0; i < propertyGetters.Length; i++)
                    {
                        object propertyValue = propertyGetters[i](item);
                        propertyValues[i] = propertyValue;
                    }
                    yield return propertyValues;
                    index++;
                }
            }
        }

        public static IEnumerable ToItemArray(this IEnumerable source, string[] propertyNames)
        {
            if (source != null)
            {
                Type[] propertyTypes;
                Func<object, object>[] propertyGetters = null;
                int index = 0;
                foreach (var item in source)
                {
                    if (index == 0)
                    {
                        Type entityType = item.GetType();
                        int propertyCount = propertyNames.Length;
                        propertyTypes = new Type[propertyCount];
                        propertyGetters = new Func<object, object>[propertyCount];
                        for (int i = 0; i < propertyCount; i++)
                        {
                            string propertyName = propertyNames[i];
                            var propertyInfo = entityType.GetProperty(propertyName);

                            Type propertyType = propertyInfo.PropertyType;
                            propertyTypes[i] = propertyType;

                            var func = GeneratePropertyGetter(entityType, propertyInfo);
                            propertyGetters[i] = func;
                        }
                    }
                    object[] propertyValues = new object[propertyGetters.Length];
                    for (int i = 0; i < propertyGetters.Length; i++)
                    {
                        object propertyValue = propertyGetters[i](item);
                        propertyValues[i] = propertyValue;
                    }
                    yield return propertyValues;
                    index++;
                }
            }
        }


        public static IEnumerable ToDataCell(this IEnumerable source, string keyPropertyName)
        {
            if (source != null)
            {
                int keyPropertyIndex = 0;
                string[] propertyNames;
                Type[] propertyTypes;
                Func<object, object>[] propertyGetters = null;
                int index = 0;
                foreach (var item in source)
                {
                    if (index == 0)
                    {
                        Type entityType = item.GetType();
                        var properties = entityType.GetProperties();
                        int propertyCount = properties.Length;
                        propertyNames = new string[propertyCount];
                        propertyTypes = new Type[propertyCount];
                        propertyGetters = new Func<object, object>[propertyCount];
                        for (int i = 0; i < propertyCount; i++)
                        {
                            var propertyInfo = properties[i];
                            string propertyName = propertyInfo.Name;
                            propertyNames[i] = propertyName;

                            Type propertyType = propertyInfo.PropertyType;
                            propertyTypes[i] = propertyType;

                            var func = GeneratePropertyGetter(entityType, propertyInfo);
                            propertyGetters[i] = func;

                            if (propertyName == keyPropertyName)
                            {
                                keyPropertyIndex = i;
                            }
                        }
                    }
                    object[] propertyValues = new object[propertyGetters.Length];
                    for (int i = 0; i < propertyGetters.Length; i++)
                    {
                        object propertyValue = propertyGetters[i](item);
                        propertyValues[i] = propertyValue;
                    }
                    yield return new { id = propertyValues[keyPropertyIndex], cell = propertyValues };
                    index++;
                }
            }
        }

        public static IEnumerable ToDataCell(this IEnumerable source, string keyPropertyName, string[] propertyNames)
        {
            if (source != null)
            {
                int keyPropertyIndex = -1;
                Func<object, object> keyPropertyGetter = null;
                Type[] propertyTypes;
                Func<object, object>[] propertyGetters = null;
                int index = 0;
                foreach (var item in source)
                {
                    if (index == 0)
                    {
                        Type entityType = item.GetType();
                        int propertyCount = propertyNames.Length;
                        propertyTypes = new Type[propertyCount];
                        propertyGetters = new Func<object, object>[propertyCount];
                        for (int i = 0; i < propertyCount; i++)
                        {
                            string propertyName = propertyNames[i];
                            var propertyInfo = entityType.GetProperty(propertyName);

                            Type propertyType = propertyInfo.PropertyType;
                            propertyTypes[i] = propertyType;

                            var func = GeneratePropertyGetter(entityType, propertyInfo);
                            propertyGetters[i] = func;

                            if (propertyName == keyPropertyName)
                            {
                                keyPropertyIndex = i;
                            }
                        }
                        if (keyPropertyIndex < 0)
                        {
                            var keyPropertyInfo = entityType.GetProperty(keyPropertyName);
                            keyPropertyGetter = GeneratePropertyGetter(entityType, keyPropertyInfo);
                        }
                    }
                    object[] propertyValues = new object[propertyGetters.Length];
                    for (int i = 0; i < propertyGetters.Length; i++)
                    {
                        object propertyValue = propertyGetters[i](item);
                        propertyValues[i] = propertyValue;
                    }
                    yield return new { id = keyPropertyIndex < 0 ? keyPropertyGetter(item) : propertyValues[keyPropertyIndex], cell = propertyValues };
                    index++;
                }
            }
        }
    }
}
