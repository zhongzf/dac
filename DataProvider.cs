using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Linq.Mapping;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using RaisingStudio.Data.Common;
using System.Configuration;
using RaisingStudio.Data.Settings;

namespace RaisingStudio.Data
{
    public class DataProvider : IDisposable
    {
        private IDbConnection connection;

        private string providerName;
        public string ProviderName
        {
            get
            {
                return this.providerName;
            }
        }

        private Database database;
        public Database Database
        {
            get
            {
                return this.database;
            }
        }

        #region Log
        public TextWriter Log
        {
            get
            {
                return this.database.Log;
            }
            set
            {
                this.database.Log = value;
            }
        }
        #endregion

        public DataProvider(IDbConnection connection, string providerName)
        {
            this.connection = connection;
            this.database = new Database(connection, providerName);
            this.providerName = providerName;
        }


        public void Dispose()
        {
            this.connection.Dispose();
            this.connection = null;
        }


        private Dictionary<Type, EntityAdapter> entityAdapters = new Dictionary<Type, EntityAdapter>();

        public virtual EntityAdapter GetEntityAdapter(Type entityType)
        {
            if (this.entityAdapters.ContainsKey(entityType))
            {
                return this.entityAdapters[entityType];
            }
            else
            {
                EntityAdapter entityAdapter = new EntityAdapter(this, entityType);
                this.entityAdapters.Add(entityType, entityAdapter);
                return entityAdapter;
            }
        }

        public const string CONFIGURATION_COMMAND_BUILDER_SECTION_NAME = "raisingstudio.data/CommandBuilder.Settings";

        public CommandBuilder CreateCommandBuilder(Expression expression, string tableName, string[] propertyNames, Type[] propertyTypes, string[] columnNames, string[] columnTypes)
        {
            CommandBuilder commandBuilder = null;
            string sectionName = CONFIGURATION_COMMAND_BUILDER_SECTION_NAME;
            CommandBuilderSettings commandBuilderSettings = (CommandBuilderSettings)ConfigurationManager.GetSection(sectionName);
            if (commandBuilderSettings != null)
            {
                for (int i = 0; i < commandBuilderSettings.Values.Count; i++)
                {
                    CommandBuilderSetting commandBuilderSetting = commandBuilderSettings.Values[i];
                    string providerName = commandBuilderSetting.ProviderName;
                    if (providerName == this.ProviderName)
                    {
                        try
                        {
                            bool useBrackets = commandBuilderSetting.UseBrackets;
                            string pagingMethod = commandBuilderSetting.PagingMethod;
                            string identityMethod = commandBuilderSetting.IdentityMethod;
                            bool supportsInsertSelectIdentity = commandBuilderSetting.SupportsInsertSelectIdentity;

                            Type commandBuilderType = commandBuilderSetting.CommandBuilderType;
                            if (commandBuilderType != null)
                            {
                                if (commandBuilderType == typeof(CommandBuilder))
                                {
                                    commandBuilder = new CommandBuilder(expression, tableName, propertyNames, propertyTypes, columnNames, columnTypes);
                                }
                                else if (commandBuilderType.IsSubclassOf(typeof(CommandBuilder)))
                                {
                                    commandBuilder = (CommandBuilder)Activator.CreateInstance(commandBuilderType, expression, tableName, propertyNames, propertyTypes, columnNames, columnTypes);
                                }
                                else
                                {
                                    throw new InvalidConstraintException("The customized CommandBuilder class must be subclass of RaisingStudio.Data.CommandBuidler.");
                                }
                            }
                            else
                            {
                                commandBuilder = new CommandBuilder(expression, tableName, propertyNames, propertyTypes, columnNames, columnTypes);
                            }

                            commandBuilder.UseBrackets = useBrackets;
                            commandBuilder.PagingMethod = pagingMethod;
                            commandBuilder.IdentityMethod = identityMethod;
                            commandBuilderSetting.SupportsInsertSelectIdentity = supportsInsertSelectIdentity;

                            break;
                        }
                        catch (Exception ex)
                        {
                            Debug.WriteLine(ex);
                        }
                    }
                }
            }
            if (commandBuilder == null)
            {
                commandBuilder = new CommandBuilder(expression, tableName, propertyNames, propertyTypes, columnNames, columnTypes);
                commandBuilder.UseBrackets = true;
                commandBuilder.PagingMethod = "ROW_NUMBER";
                commandBuilder.IdentityMethod = "IDENTITY";
                commandBuilder.SupportsInsertSelectIdentity = false;
            }
            return commandBuilder;
        }


        public static Type GetEntityType(Expression expression)
        {
            if (expression is MethodCallExpression)
            {
                MethodCallExpression m = expression as MethodCallExpression;
                if (m.Method.Name == "Select")
                {
                    return GetEntityType(m.Arguments[0]);
                }
                else if (m.Method.Name == "Count")
                {
                    return GetEntityType(m.Arguments[0]);
                }
                else if (m.Method.Name == "Average")
                {
                    return GetEntityType(m.Arguments[0]);
                }
                else if ((m.Method.Name == "Max") || (m.Method.Name == "Min") || (m.Method.Name == "Sum"))
                {
                    return GetEntityType(m.Arguments[0]);
                }
            }
            if (expression.Type.IsGenericType)
            {
                return expression.Type.GetGenericArguments()[0];
            }
            else
            {
                return expression.Type;
            }
        }

        public static void GetDataStructure(IDataReader dataReader, out string[] columnNames, out Type[] propertyTypes, out string[] columnTypes)
        {
            // TODO: dataReader.GetSchemaTable()
            int fieldCount = dataReader.FieldCount;
            columnNames = new string[fieldCount];
            propertyTypes = new Type[fieldCount];
            columnTypes = new string[fieldCount];
            for (int i = 0; i < fieldCount; i++)
            {
                columnNames[i] = dataReader.GetName(i);
                columnTypes[i] = dataReader.GetDataTypeName(i);

                Type fieldType = dataReader.GetFieldType(i);
                if (fieldType != typeof(string) && (fieldType != typeof(byte[])))
                {
                    Type nullableType = typeof(Nullable<>);
                    propertyTypes[i] = nullableType.MakeGenericType(fieldType);
                }
                else
                {
                    propertyTypes[i] = fieldType;
                }
            }
        }


        public object Execute(Expression expression)
        {
            Type entityType = GetEntityType(expression);
            EntityAdapter entityAdapter = GetEntityAdapter(entityType);
            return entityAdapter.Execute(expression);
        }


        public IEnumerable Query(Expression expression)
        {
            Type entityType = GetEntityType(expression);
            EntityAdapter entityAdapter = GetEntityAdapter(entityType);
            var instance = Expression.Parameter(typeof(EntityAdapter), "instance");
            var parameter = Expression.Parameter(typeof(Expression), "expression");
            var call = Expression.Call(instance, "Query", new Type[] { entityType }, parameter);
            var lambda = Expression.Lambda<Func<EntityAdapter, Expression, IEnumerable>>(call, instance, parameter);
            return lambda.Compile()(entityAdapter, expression);
        }

        public IEnumerable Query(Command command)
        {
            var dataReader = this.database.ExecuteReader(command);
            string[] columnNames;
            Type[] propertyTypes;
            string[] columnTypes;
            GetDataStructure(dataReader, out columnNames, out propertyTypes, out columnTypes);
            string[] propertyNames = columnNames;

            Type entityType = TypeManager.CreateDynamicType("_", "<>_DynamicType", null, propertyNames, propertyTypes);
            EntityAdapter entityAdapter = new EntityAdapter(this, entityType, null, propertyNames, propertyTypes, columnNames, columnTypes);
            var instance = Expression.Parameter(typeof(EntityAdapter), "instance");
            var parameter = Expression.Parameter(typeof(IDataReader), "dataReader");
            var call = Expression.Call(instance, "GetEnumerator", new Type[] { entityType }, parameter);
            var lambda = Expression.Lambda<Func<EntityAdapter, IDataReader, IEnumerable>>(call, instance, parameter);
            return lambda.Compile()(entityAdapter, dataReader);
        }

        public IEnumerable<T> Query<T>(Command command) where T : new()
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            var dataReader = this.database.ExecuteReader(command);
            string[] columnNames;
            Type[] propertyTypes;
            string[] columnTypes;
            GetDataStructure(dataReader, out columnNames, out propertyTypes, out columnTypes);
            string[] columns = entityAdapter.GetMappingProperties(columnNames);
            return entityAdapter.GetEnumerator<T>(dataReader, columns);
        }

        public int Insert<T>(T dataObject)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.Insert<T>(dataObject);
        }

        public int Delete<T>(Expression expression)
        {
            Type entityType = typeof(T);
            EntityAdapter entityAdapter = GetEntityAdapter(entityType);
            return entityAdapter.Delete(expression);
        }

        public int Delete<T>(params object[] primaryKeys)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.Delete(primaryKeys);
        }

        public int Delete<T>(T dataObject)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.Delete<T>(dataObject);
        }

        public int Update<T>(T dataObject)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.Update<T>(dataObject);
        }

        public int Update<T>(object[] primaryKeys, T dataObject)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.Update<T>(primaryKeys, dataObject);
        }

        public int Update<T>(T dataObject, Expression expression)
        {
            Type entityType = typeof(T);
            EntityAdapter entityAdapter = GetEntityAdapter(entityType);
            return entityAdapter.Update<T>(dataObject, expression);
        }

        public T GetEntity<T>(params object[] primaryKeys) where T : new()
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.GetEntity<T>(primaryKeys);
        }

        public IEnumerable<T> Query<T>(string[] columns, Expression expression) where T : new()
        {
            Type entityType = typeof(T);
            EntityAdapter entityAdapter = GetEntityAdapter(entityType);
            return entityAdapter.Query<T>(columns, expression);
        }

        public int Update<T>(T dataObject, string[] columns)
        {
            Type entityType = typeof(T);
            EntityAdapter entityAdapter = GetEntityAdapter(entityType);
            return entityAdapter.Update<T>(dataObject, columns);
        }

        public int Update<T>(T dataObject, string[] columns, Expression expression)
        {
            Type entityType = typeof(T);
            EntityAdapter entityAdapter = GetEntityAdapter(entityType);
            return entityAdapter.Update<T>(dataObject, columns, expression);
        }

        public int Insert<T>(T dataObject, string[] columns)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.Insert<T>(dataObject, columns);
        }


        public int GetCount<T>(Expression expression = null)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.GetCount<T>(expression);
        }

        public long GetLongCount<T>(Expression expression = null)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.GetLongCount<T>(expression);
        }

        public object GetSum<T>(string column, Expression expression = null)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.GetSum<T>(column, expression);
        }

        public object GetAvg<T>(string column, Expression expression = null)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.GetAvg<T>(column, expression);
        }

        public object GetMin<T>(string column, Expression expression = null)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.GetMin<T>(column, expression);
        }

        public object GetMax<T>(string column, Expression expression = null)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.GetMax<T>(column, expression);
        }

        public bool Exists<T>(T dataObject)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.Exists<T>(dataObject);
        }

        public bool Exists<T>(params object[] primaryKeys)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.Exists<T>(primaryKeys);
        }


        public Command GetMappingCommand<T>(Command command)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.GetMappingCommand<T>(command);
        }


        public IEnumerable EntityExecute<T>(Command command)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            var dataReader = entityAdapter.EntityReader<T>(command);
            string[] columnNames;
            Type[] propertyTypes;
            string[] columnTypes;
            GetDataStructure(dataReader, out columnNames, out propertyTypes, out columnTypes);
            string[] propertyNames = columnNames;

            Type entityType = TypeManager.CreateDynamicType("_", "<>_DynamicType", null, propertyNames, propertyTypes);
            entityAdapter = new EntityAdapter(this, entityType, null, propertyNames, propertyTypes, columnNames, columnTypes);
            var instance = Expression.Parameter(typeof(EntityAdapter), "instance");
            var parameter = Expression.Parameter(typeof(IDataReader), "dataReader");
            var call = Expression.Call(instance, "GetEnumerator", new Type[] { entityType }, parameter);
            var lambda = Expression.Lambda<Func<EntityAdapter, IDataReader, IEnumerable>>(call, instance, parameter);
            return lambda.Compile()(entityAdapter, dataReader);
        }

        public IEnumerable<T> EntityQuery<T>(Command command) where T : new()
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.EntityQuery<T>(command);
        }

        public int EntityNonQuery<T>(Command command)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.EntityNonQuery<T>(command);
        }

        public object EntityScalar<T>(Command command)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.EntityScalar<T>(command);
        }

        public IDataReader EntityReader<T>(Command command)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.EntityReader<T>(command);
        }


        public object GetIdentity<T>()
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            return entityAdapter.GetIdentity<T>();
        }


        public IEnumerable Query(string tableName, string[] columnNames, string[] searchColumns, string[] searchOperators, object[] searchValues, string[] logicalOperators, string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount)
        {
            CommandBuilder commandBuilder = CreateCommandBuilder(null, tableName, null, null, columnNames, null);
            Command command = commandBuilder.GetQueryCommand(searchColumns, searchOperators, searchValues, logicalOperators, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
            return Query(command);
        }

        private IEnumerable<T> Query<T>(EntityAdapter entityAdapter, string tableName, string[] columnNames, string[] searchColumns, string[] searchOperators, object[] searchValues, string[] logicalOperators, string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount) where T : new()
        {
            CommandBuilder commandBuilder = CreateCommandBuilder(null, tableName, null, null, columnNames, null);
            GetMappingColumns(entityAdapter, searchColumns, sortColumns);
            Command command = commandBuilder.GetQueryCommand(searchColumns, searchOperators, searchValues, logicalOperators, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
            return Query<T>(command);
        }

        private static void GetMappingColumns(EntityAdapter entityAdapter, string[] searchColumns, string[] sortColumns)
        {
            string[] propertyNames = entityAdapter.PropertyNames;
            if ((searchColumns != null) && (searchColumns.Length > 0))
            {
                for (int i = 0; i < searchColumns.Length; i++)
                {
                    string searchColumn = searchColumns[i];
                    if (!string.IsNullOrWhiteSpace(searchColumn))
                    {
                        searchColumns[i] = entityAdapter.ColumnsNames[Array.IndexOf(propertyNames, searchColumn)];
                    }
                }
            }
            if ((sortColumns != null) && (sortColumns.Length > 0))
            {
                for (int i = 0; i < sortColumns.Length; i++)
                {
                    string sortColumn = sortColumns[i];
                    if (!string.IsNullOrWhiteSpace(sortColumn))
                    {
                        sortColumns[i] = entityAdapter.ColumnsNames[Array.IndexOf(propertyNames, sortColumn)];
                    }
                }
            }
        }

        public IEnumerable<T> Query<T>(string[] searchColumns, string[] searchOperators, object[] searchValues, string[] logicalOperators, string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount) where T : new()
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            string tableName = entityAdapter.TableName;
            string[] columnNames = entityAdapter.ColumnsNames;
            return Query<T>(entityAdapter, tableName, columnNames, searchColumns, searchOperators, searchValues, logicalOperators, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
        }

        public IEnumerable<T> Query<T>(string[] columns, string[] searchColumns, string[] searchOperators, object[] searchValues, string[] logicalOperators, string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount) where T : new()
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            string tableName = entityAdapter.TableName;
            string[] columnNames = columns.Select(s => entityAdapter.ColumnsNames[Array.IndexOf(entityAdapter.PropertyNames, s)]).ToArray();
            return Query<T>(entityAdapter, tableName, columnNames, searchColumns, searchOperators, searchValues, logicalOperators, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
        }


        public int GetCount(string tableName, string[] searchColumns, string[] searchOperators, object[] searchValues, string[] logicalOperators)
        {
            CommandBuilder commandBuilder = CreateCommandBuilder(null, tableName, null, null, null, null);
            Command command = commandBuilder.GetCountCommand(searchColumns, searchOperators, searchValues, logicalOperators);
            return Convert.ToInt32(this.database.ExecuteScalar(command));
        }

        public int GetCount<T>(string[] searchColumns, string[] searchOperators, object[] searchValues, string[] logicalOperators)
        {
            EntityAdapter entityAdapter = GetEntityAdapter(typeof(T));
            string tableName = entityAdapter.TableName;
            CommandBuilder commandBuilder = CreateCommandBuilder(null, tableName, null, null, null, null);
            GetMappingColumns(entityAdapter, searchColumns, null);
            Command command = commandBuilder.GetCountCommand(searchColumns, searchOperators, searchValues, logicalOperators);
            return Convert.ToInt32(this.database.ExecuteScalar(command));
        }
    }
}
