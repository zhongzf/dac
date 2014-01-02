using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using RaisingStudio.Data.Common;
using RaisingStudio.Data.Linq;
using System.Diagnostics;
using RaisingStudio.Data.Settings;

namespace RaisingStudio.Data
{
    public class DataContext : IDisposable
    {
        private IDbConnection connection;
        public IDbConnection Connection
        {
            get
            {
                return this.connection;
            }
        }

        private static DbConnection CreateConnection(string connectionString, string providerName)
        {
            DbProviderFactory providerFactory = DbProviderFactories.GetFactory(providerName);
            var connection = providerFactory.CreateConnection();
            connection.ConnectionString = connectionString;
            return connection;
        }

        public DataContext()
        {
            if (ConfigurationManager.ConnectionStrings.Count > 0)
            {
                ConnectionStringSettings connectionStringSettings = ConfigurationManager.ConnectionStrings[0];
                this.connection = CreateConnection(connectionStringSettings.ConnectionString, connectionStringSettings.ProviderName);
                this.provider = new DataProvider(this.connection, connectionStringSettings.ProviderName);
            }
            else
            {
                string connectionString = @"data source=.\SQLEXPRESS;Integrated Security=SSPI;AttachDBFilename=|DataDirectory|aspnetdb.mdf;User Instance=true";
                string providerName = "System.Data.SqlClient";
                this.connection = CreateConnection(connectionString, providerName);
                this.provider = new DataProvider(this.connection, providerName);
            }
        }

        public DataContext(string configOrFileOrServerOrConnection)
        {
            if (string.IsNullOrEmpty(configOrFileOrServerOrConnection))
            {
                throw new ArgumentException(configOrFileOrServerOrConnection);
            }
            ConnectionStringSettings connectionStringSettings = ConfigurationManager.ConnectionStrings[configOrFileOrServerOrConnection];
            if (connectionStringSettings != null)
            {
                this.connection = CreateConnection(connectionStringSettings.ConnectionString, connectionStringSettings.ProviderName);
                this.provider = new DataProvider(this.connection, connectionStringSettings.ProviderName);
            }
            else
            {
                if (configOrFileOrServerOrConnection.EndsWith(".mdb", StringComparison.OrdinalIgnoreCase) || configOrFileOrServerOrConnection.EndsWith(".accdb", StringComparison.OrdinalIgnoreCase))
                {
                    string providerName = "System.Data.OleDb";
                    this.connection = new OleDbConnection(configOrFileOrServerOrConnection);
                    this.provider = new DataProvider(this.connection, providerName);
                }
                else if (configOrFileOrServerOrConnection.EndsWith(".db", StringComparison.OrdinalIgnoreCase) || configOrFileOrServerOrConnection.EndsWith(".db3", StringComparison.OrdinalIgnoreCase))
                {
                    string providerName = "System.Data.SQLite";
                    this.connection = CreateConnection(configOrFileOrServerOrConnection, providerName);
                    this.provider = new DataProvider(this.connection, providerName);
                }
                else if (configOrFileOrServerOrConnection.EndsWith(".mdf", StringComparison.OrdinalIgnoreCase))
                {
                    string providerName = "System.Data.SqlClient";
                    this.connection = CreateConnection(configOrFileOrServerOrConnection, providerName);
                    this.provider = new DataProvider(this.connection, providerName);
                }
                else if (configOrFileOrServerOrConnection.EndsWith(".sdf", StringComparison.OrdinalIgnoreCase))
                {
                    string providerName = "System.Data.SqlServerCe.4.0";
                    this.connection = CreateConnection(configOrFileOrServerOrConnection, providerName);
                    this.provider = new DataProvider(this.connection, providerName);
                }
                else
                {
                    string providerName = "System.Data.SqlClient";
                    this.connection = CreateConnection(configOrFileOrServerOrConnection, providerName);
                    this.provider = new DataProvider(this.connection, providerName);
                }
            }
        }

        public DataContext(string connectionString, string providerName)
        {
            this.connection = CreateConnection(connectionString, providerName);
            this.provider = new DataProvider(this.connection, providerName);
        }

        public DataContext(IDbConnection connection)
        {
            this.connection = connection;
            this.provider = new DataProvider(this.connection, GetConfigurationSettingsProviderName());
        }

        public DataContext(IDbConnection connection, string providerName)
        {
            this.connection = connection;
            this.provider = new DataProvider(this.connection, providerName);
        }


        private string GetConfigurationSettingsProviderName()
        {
            string providerName = GetCommandBuilderSettingsProviderName();
            if (string.IsNullOrWhiteSpace(providerName))
            {
                providerName = GetCommandConverterSettingsProviderName();
                if (string.IsNullOrWhiteSpace(providerName))
                {
                    if (ConfigurationManager.ConnectionStrings.Count > 0)
                    {
                        for (int i = 0; i < ConfigurationManager.ConnectionStrings.Count; i++)
                        {
                            ConnectionStringSettings connectionStringSettings = ConfigurationManager.ConnectionStrings[i];                            
                            if (IsCurrentConnectionType(connectionStringSettings.ProviderName))
                            {
                                providerName = connectionStringSettings.ProviderName;
                            }
                        }
                    }
                }
            }
            return providerName;
        }

        public const string CONFIGURATION_COMMAND_BUILDER_SECTION_NAME = DataProvider.CONFIGURATION_COMMAND_BUILDER_SECTION_NAME;
        private string GetCommandBuilderSettingsProviderName()
        {
            string sectionName = CONFIGURATION_COMMAND_BUILDER_SECTION_NAME;
            CommandBuilderSettings commandBuilderSettings = (CommandBuilderSettings)ConfigurationManager.GetSection(sectionName);
            if (commandBuilderSettings != null)
            {
                for (int i = 0; i < commandBuilderSettings.Values.Count; i++)
                {
                    CommandBuilderSetting commandBuilderSetting = commandBuilderSettings.Values[i];
                    string providerName = commandBuilderSetting.ProviderName;
                    if (IsCurrentConnectionType(providerName))
                    {
                        return providerName;
                    }
                }
            }
            return null;
        }

        public const string CONFIGURATION_COMMAND_CONVERTER_SECTION_NAME = CommandConverter.CONFIGURATION_COMMAND_CONVERTER_SECTION_NAME;
        private string GetCommandConverterSettingsProviderName()
        {
            string sectionName = CONFIGURATION_COMMAND_CONVERTER_SECTION_NAME;
            CommandConverterSettings commandConverterSettings = (CommandConverterSettings)ConfigurationManager.GetSection(sectionName);
            if (commandConverterSettings != null)
            {
                for (int i = 0; i < commandConverterSettings.Values.Count; i++)
                {
                    CommandConverterSetting commandConverterSetting = commandConverterSettings.Values[i];
                    string providerName = commandConverterSetting.ProviderName;
                    if (IsCurrentConnectionType(providerName))
                    {
                        return providerName;
                    }
                }
            }
            return null;
        }

        private bool IsCurrentConnectionType(string providerName)
        {
            if (!string.IsNullOrEmpty(providerName))
            {
                try
                {
                    DbProviderFactory providerFactory = DbProviderFactories.GetFactory(providerName);
                    if (providerFactory != null)
                    {
                        var connection = providerFactory.CreateConnection();
                        if ((this.connection != null) && (connection != null))
                        {
                            if (this.connection.GetType() == connection.GetType())
                            {
                                return true;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Debug.WriteLine(ex);
                }
            }
            return false;
        }


        private DataProvider provider;
        public DataProvider Provider
        {
            get
            {
                return this.provider;
            }
        }


        #region Log
        public TextWriter Log
        {
            get
            {
                this.CheckDispose();
                return this.provider.Log;
            }
            set
            {
                this.CheckDispose();
                this.provider.Log = value;
            }
        }
        #endregion

        #region Transaction
        public IDbTransaction Transaction
        {
            get
            {
                this.CheckDispose();
                return this.provider.Database.Transaction;
            }
            set
            {
                this.CheckDispose();
                this.provider.Database.Transaction = value;
            }
        }

        /// <summary>
        /// Begin Database transaction.
        /// </summary>
        /// <returns>The ID of transaction.</returns>
        public virtual IDbTransaction BeginTransaction()
        {
            return this.provider.Database.BeginTransaction();
        }

        /// <summary>
        /// Begin Database transaction.
        /// </summary>
        /// <param name="isolationLevel">Specifies the isolation level for the transaction.</param>
        /// <returns>The ID of transaction.</returns>
        public IDbTransaction BeginTransaction(IsolationLevel isolationLevel)
        {
            return this.provider.Database.BeginTransaction(isolationLevel);
        }

        /// <summary>
        /// Commit Database transaction.
        /// </summary>
        public void CommitTransaction()
        {
            this.provider.Database.CommitTransaction();
        }

        /// <summary>
        /// Rollback transaction.
        /// </summary>
        public void RollbackTransaction()
        {
            this.provider.Database.RollbackTransaction();
        }
        #endregion

        #region Dispose
        private bool disposed;
        internal void CheckDispose()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException("DataContext");
            }
        }

        public void Dispose()
        {
            this.disposed = true;
            this.Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (this.provider != null)
                {
                    this.provider.Dispose();
                    this.provider = null;
                }
            }
        }
        #endregion


        public Query<T> GetQuery<T>()
        {
            return new Query<T>(this);
        }


        public IEnumerable Query(Command command)
        {
            return this.provider.Query(command);
        }

        public IEnumerable Query(string commandText)
        {
            return this.provider.Query(new Command(commandText));
        }

        public IEnumerable<T> Query<T>(Command command) where T : new()
        {
            return this.provider.Query<T>(command);
        }

        public IEnumerable<T> Query<T>(string commandText) where T : new()
        {
            return this.provider.Query<T>(new Command(commandText));
        }


        private static string[] GetColumns<T>(Expression<Func<T, object>>[] columnExpressions)
        {
            string[] columns = new string[columnExpressions.Length];
            for (int i = 0; i < columnExpressions.Length; i++)
            {
                var columnExpression = columnExpressions[i];
                columns[i] = GetColumn<T>(columnExpression);
            }
            return columns;
        }

        private static string GetColumn<T>(Expression<Func<T, object>> columnExpression)
        {
            if (columnExpression.Body is UnaryExpression)
            {
                dynamic body = (columnExpression.Body as UnaryExpression).Operand;
                return body.Member.Name;
            }
            else
            {
                dynamic body = columnExpression.Body;
                return body.Member.Name;
            }
        }


        public int Insert<T>(T dataObject)
        {
            return this.provider.Insert<T>(dataObject);
        }


        public int Delete<T>(Expression<Func<T, bool>> condition)
        {
            return this.provider.Delete<T>(condition);
        }

        public int Delete<T>(Expression expression)
        {
            return this.provider.Delete<T>(expression);
        }

        public int Delete<T>(params object[] primaryKeys)
        {
            return this.provider.Delete<T>(primaryKeys);
        }

        public int Delete<T>(T dataObject)
        {
            return this.provider.Delete<T>(dataObject);
        }


        public int Update<T>(T dataObject)
        {
            return this.provider.Update<T>(dataObject);
        }

        public int Update<T>(object[] primaryKeys, T dataObject)
        {
            return this.provider.Update<T>(primaryKeys, dataObject);
        }

        public int Update<T>(T dataObject, Expression expression)
        {
            return this.provider.Update<T>(dataObject, expression);
        }

        public int Update<T>(T dataObject, Expression<Func<T, bool>> condition)
        {
            return this.provider.Update<T>(dataObject, condition);
        }


        public T GetEntity<T>(params object[] primaryKeys) where T : new()
        {
            return this.provider.GetEntity<T>(primaryKeys);
        }


        public IEnumerable<T> Query<T>(Expression<Func<T, object>>[] columnExpressions, Expression<Func<T, bool>> condition) where T : new()
        {
            string[] columns = GetColumns<T>(columnExpressions);
            return this.provider.Query<T>(columns, condition);
        }

        public IEnumerable<T> Query<T>(string[] columns, Expression<Func<T, bool>> condition) where T : new()
        {
            return this.provider.Query<T>(columns, condition);
        }

        public IEnumerable<T> Query<T>(string[] columns, Expression expression) where T : new()
        {
            return this.provider.Query<T>(columns, expression);
        }

        public IEnumerable<T> Query<T>(Expression<Func<T, bool>> condition) where T : new()
        {
            return this.provider.Query<T>(null, condition);
        }

        public IEnumerable<T> Query<T>(Expression expression) where T : new()
        {
            return this.provider.Query<T>(null, expression);
        }

        public IEnumerable<T> Query<T>() where T : new()
        {
            return this.provider.Query<T>(null, null);
        }


        public int Update<T>(T dataObject, Expression<Func<T, object>>[] columnExpressions)
        {
            string[] columns = GetColumns<T>(columnExpressions);
            return Update<T>(dataObject, columns);
        }

        public int Update<T>(T dataObject, string[] columns)
        {
            return this.provider.Update<T>(dataObject, columns);
        }

        public int Update<T>(T dataObject, Expression<Func<T, object>>[] columnExpressions, Expression<Func<T, bool>> condition)
        {
            string[] columns = GetColumns<T>(columnExpressions);
            return this.provider.Update<T>(dataObject, columns, condition);
        }

        public int Update<T>(T dataObject, string[] columns, Expression<Func<T, bool>> condition)
        {
            return this.provider.Update<T>(dataObject, columns, condition);
        }

        public int Update<T>(T dataObject, string[] columns, Expression expression)
        {
            return this.provider.Update<T>(dataObject, columns, expression);
        }


        public int Insert<T>(T dataObject, Expression<Func<T, object>>[] columnExpressions)
        {
            string[] columns = GetColumns<T>(columnExpressions);
            return Insert<T>(dataObject, columns);
        }

        public int Insert<T>(T dataObject, string[] columns)
        {
            return this.provider.Insert<T>(dataObject, columns);
        }


        public int GetCount<T>(Expression<Func<T, bool>> condition)
        {
            return this.provider.GetCount<T>(condition);
        }

        public int GetCount<T>(Expression expression = null)
        {
            return this.provider.GetCount<T>(expression);
        }


        public long GetLongCount<T>(Expression<Func<T, bool>> condition)
        {
            return this.provider.GetLongCount<T>(condition);
        }

        public long GetLongCount<T>(Expression expression = null)
        {
            return this.provider.GetLongCount<T>(expression);
        }


        public object GetMin<T>(Expression<Func<T, object>> columnExpression, Expression<Func<T, bool>> condition)
        {
            return this.provider.GetMin<T>(GetColumn<T>(columnExpression), condition);
        }

        public object GetMin<T>(string column, Expression<Func<T, bool>> condition)
        {
            return this.provider.GetMin<T>(column, condition);
        }

        public object GetMin<T>(Expression<Func<T, object>> columnExpression, Expression expression = null)
        {
            return this.provider.GetMin<T>(GetColumn<T>(columnExpression), expression);
        }

        public object GetMin<T>(string column, Expression expression = null)
        {
            return this.provider.GetMin<T>(column, expression);
        }


        public object GetMax<T>(Expression<Func<T, object>> columnExpression, Expression<Func<T, bool>> condition)
        {
            return this.provider.GetMax<T>(GetColumn<T>(columnExpression), condition);
        }

        public object GetMax<T>(string column, Expression<Func<T, bool>> condition)
        {
            return this.provider.GetMax<T>(column, condition);
        }

        public object GetMax<T>(Expression<Func<T, object>> columnExpression, Expression expression = null)
        {
            return this.provider.GetMax<T>(GetColumn<T>(columnExpression), expression);
        }

        public object GetMax<T>(string column, Expression expression = null)
        {
            return this.provider.GetMax<T>(column, expression);
        }


        public object GetSum<T>(Expression<Func<T, object>> columnExpression, Expression<Func<T, bool>> condition)
        {
            return this.provider.GetSum<T>(GetColumn<T>(columnExpression), condition);
        }

        public object GetSum<T>(string column, Expression<Func<T, bool>> condition)
        {
            return this.provider.GetSum<T>(column, condition);
        }

        public object GetSum<T>(Expression<Func<T, object>> columnExpression, Expression expression = null)
        {
            return this.provider.GetSum<T>(GetColumn<T>(columnExpression), expression);
        }

        public object GetSum<T>(string column, Expression expression = null)
        {
            return this.provider.GetSum<T>(column, expression);
        }


        public object GetAvg<T>(Expression<Func<T, object>> columnExpression, Expression<Func<T, bool>> condition)
        {
            return this.provider.GetAvg<T>(GetColumn<T>(columnExpression), condition);
        }

        public object GetAvg<T>(string column, Expression<Func<T, bool>> condition)
        {
            return this.provider.GetAvg<T>(column, condition);
        }

        public object GetAvg<T>(Expression<Func<T, object>> columnExpression, Expression expression = null)
        {
            return this.provider.GetAvg<T>(GetColumn<T>(columnExpression), expression);
        }

        public object GetAvg<T>(string column, Expression expression = null)
        {
            return this.provider.GetAvg<T>(column, expression);
        }


        public bool Exists<T>(T dataObject)
        {
            return this.provider.Exists<T>(dataObject);
        }

        public bool Exists<T>(params object[] primaryKeys)
        {
            return this.provider.Exists<T>(primaryKeys);
        }


        public bool Exists<T>(Expression<Func<T, bool>> condition)
        {
            return this.provider.GetCount<T>(condition) > 0;
        }

        public bool Exists<T>(Expression expression = null)
        {
            return this.provider.GetCount<T>(expression) > 0;
        }


        public int Save<T>(T dataObject)
        {
            if (this.Transaction == null)
            {
                this.BeginTransaction();
                try
                {
                    int result;
                    if (this.Exists<T>(dataObject))
                    {
                        result = this.Update<T>(dataObject);
                    }
                    else
                    {
                        result = this.Insert<T>(dataObject);
                    }
                    this.CommitTransaction();
                    return result;
                }
                catch (Exception ex)
                {
                    this.RollbackTransaction();
                    throw ex;
                }
            }
            else
            {
                if (this.Exists<T>(dataObject))
                {
                    return this.Update<T>(dataObject);
                }
                else
                {
                    return this.Insert<T>(dataObject);
                }
            }
        }

        public int Save<T>(T dataObject, Expression<Func<T, object>>[] columnExpressions)
        {
            string[] columns = GetColumns<T>(columnExpressions);
            return Save<T>(dataObject, columns);
        }

        public int Save<T>(T dataObject, string[] columns)
        {
            if (this.Transaction == null)
            {
                this.BeginTransaction();
                try
                {
                    int result;
                    if (this.Exists<T>(dataObject))
                    {
                        result = this.Update<T>(dataObject, columns);
                    }
                    else
                    {
                        result = this.Insert<T>(dataObject, columns);
                    }
                    this.CommitTransaction();
                    return result;
                }
                catch (Exception ex)
                {
                    this.RollbackTransaction();
                    throw ex;
                }
            }
            else
            {
                if (this.Exists<T>(dataObject))
                {
                    return this.Update<T>(dataObject, columns);
                }
                else
                {
                    return this.Insert<T>(dataObject, columns);
                }
            }
        }


        public Command GetMappingCommand<T>(Command command)
        {
            return this.provider.GetMappingCommand<T>(command);
        }

        public IEnumerable EntityExecute<T>(Command command)
        {
            return this.provider.EntityExecute<T>(command);
        }

        public IEnumerable EntityExecute<T>(string commandText)
        {
            return this.provider.EntityExecute<T>(new Command(commandText));
        }


        public IEnumerable<T> EntityQuery<T>(Command command) where T : new()
        {
            return this.provider.EntityQuery<T>(command);
        }

        public IEnumerable<T> EntityQuery<T>(string commandText) where T : new()
        {
            return this.provider.EntityQuery<T>(new Command(commandText));
        }


        public int EntityNonQuery<T>(Command command)
        {
            return this.provider.EntityNonQuery<T>(command);
        }

        public object EntityScalar<T>(Command command)
        {
            return this.provider.EntityScalar<T>(command);
        }

        public IDataReader EntityReader<T>(Command command)
        {
            return this.provider.EntityReader<T>(command);
        }


        public int EntityNonQuery<T>(string commandText)
        {
            return this.provider.EntityNonQuery<T>(new Command(commandText));
        }

        public object EntityScalar<T>(string commandText)
        {
            return this.provider.EntityScalar<T>(new Command(commandText));
        }

        public IDataReader EntityReader<T>(string commandText)
        {
            return this.provider.EntityReader<T>(new Command(commandText));
        }


        public object GetIdentity<T>()
        {
            return this.provider.GetIdentity<T>();
        }


        public int ExecuteNonQuery(Command command)
        {
            return this.provider.Database.ExecuteNonQuery(command);
        }

        public object ExecuteScalar(Command command)
        {
            return this.provider.Database.ExecuteScalar(command);
        }

        public IDataReader ExecuteReader(Command command)
        {
            return this.provider.Database.ExecuteReader(command);
        }


        public IEnumerable Query(string tableName, string[] columnNames, string[] searchColumns, string[] searchOperators, object[] searchValues, string[] logicalOperators, string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount)
        {
            return this.provider.Query(tableName, columnNames, searchColumns, searchOperators, searchValues, logicalOperators, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable Query(string tableName, string[] searchColumns, string[] searchOperators, object[] searchValues, string[] logicalOperators, string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount)
        {
            return this.provider.Query(tableName, null, searchColumns, searchOperators, searchValues, logicalOperators, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable Query(string tableName, string[] columnNames, string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount)
        {
            return this.provider.Query(tableName, columnNames, null, null, null, null, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable Query(string tableName, string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount)
        {
            return this.provider.Query(tableName, null, null, null, null, null, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable Query(string tableName, string[] columnNames, int pagingSkipCount, int pagingTakeCount)
        {
            return this.provider.Query(tableName, columnNames, null, null, null, null, null, null, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable Query(string tableName, int pagingSkipCount, int pagingTakeCount)
        {
            return this.provider.Query(tableName, null, null, null, null, null, null, null, pagingSkipCount, pagingTakeCount);
        }

        public IEnumerable<T> Query<T>(string[] columns, string[] searchColumns, string[] searchOperators, object[] searchValues, string[] logicalOperators, string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount) where T : new()
        {
            return this.provider.Query<T>(columns, searchColumns, searchOperators, searchValues, logicalOperators, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable<T> Query<T>(string[] searchColumns, string[] searchOperators, object[] searchValues, string[] logicalOperators, string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount) where T : new()
        {
            return this.provider.Query<T>(searchColumns, searchOperators, searchValues, logicalOperators, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable<T> Query<T>(string[] columns,string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount) where T : new()
        {
            return this.provider.Query<T>(columns, null, null, null, null, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable<T> Query<T>(string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount) where T : new()
        {
            return this.provider.Query<T>(null, null, null, null, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable<T> Query<T>(string[] columns, int pagingSkipCount, int pagingTakeCount) where T : new()
        {
            return this.provider.Query<T>(columns, null, null, null, null, null, null, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable<T> Query<T>(int pagingSkipCount, int pagingTakeCount) where T : new()
        {
            return this.provider.Query<T>(null, null, null, null, null, null, pagingSkipCount, pagingTakeCount);
        }


        public int GetCount(string tableName, string[] searchColumns, string[] searchOperators, object[] searchValues, string[] logicalOperators)
        {
            return this.provider.GetCount(tableName, searchColumns, searchOperators, searchValues, logicalOperators);
        }

        public int GetCount<T>(string[] searchColumns, string[] searchOperators, object[] searchValues, string[] logicalOperators)
        {
            return this.provider.GetCount<T>(searchColumns, searchOperators, searchValues, logicalOperators);
        }


        public IEnumerable Query(string tableName, string[] columnNames, string[] searchColumns, string[] searchOperators, object[] searchValues, string[] logicalOperators, string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount, out int count)
        {
            count = this.provider.GetCount(tableName, searchColumns, searchOperators, searchValues, logicalOperators);
            return this.provider.Query(tableName, columnNames, searchColumns, searchOperators, searchValues, logicalOperators, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable Query(string tableName, string[] searchColumns, string[] searchOperators, object[] searchValues, string[] logicalOperators, string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount, out int count)
        {
            count = this.provider.GetCount(tableName, searchColumns, searchOperators, searchValues, logicalOperators);
            return this.provider.Query(tableName, null, searchColumns, searchOperators, searchValues, logicalOperators, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable Query(string tableName, string[] columnNames, string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount, out int count)
        {
            count = this.provider.GetCount(tableName, null, null, null, null);
            return this.provider.Query(tableName, columnNames, null, null, null, null, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable Query(string tableName, string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount, out int count)
        {
            count = this.provider.GetCount(tableName, null, null, null, null);
            return this.provider.Query(tableName, null, null, null, null, null, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable Query(string tableName, string[] columnNames, int pagingSkipCount, int pagingTakeCount, out int count)
        {
            count = this.provider.GetCount(tableName, null, null, null, null);
            return this.provider.Query(tableName, columnNames, null, null, null, null, null, null, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable Query(string tableName, int pagingSkipCount, int pagingTakeCount, out int count)
        {
            count = this.provider.GetCount(tableName, null, null, null, null);
            return this.provider.Query(tableName, null, null, null, null, null, null, null, pagingSkipCount, pagingTakeCount);
        }

        public IEnumerable<T> Query<T>(string[] columns, string[] searchColumns, string[] searchOperators, object[] searchValues, string[] logicalOperators, string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount, out int count) where T : new()
        {
            count = this.provider.GetCount<T>(searchColumns, searchOperators, searchValues, logicalOperators);
            return this.provider.Query<T>(columns, searchColumns, searchOperators, searchValues, logicalOperators, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable<T> Query<T>(string[] searchColumns, string[] searchOperators, object[] searchValues, string[] logicalOperators, string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount, out int count) where T : new()
        {
            count = this.provider.GetCount<T>(searchColumns, searchOperators, searchValues, logicalOperators);
            return this.provider.Query<T>(searchColumns, searchOperators, searchValues, logicalOperators, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable<T> Query<T>(string[] columns, string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount, out int count) where T : new()
        {
            count = this.provider.GetCount<T>(null, null, null, null);
            return this.provider.Query<T>(columns, null, null, null, null, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable<T> Query<T>(string[] sortColumns, string[] sortOrdering, int pagingSkipCount, int pagingTakeCount, out int count) where T : new()
        {
            count = this.provider.GetCount<T>(null, null, null, null);
            return this.provider.Query<T>(null, null, null, null, sortColumns, sortOrdering, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable<T> Query<T>(string[] columns, int pagingSkipCount, int pagingTakeCount, out int count) where T : new()
        {
            count = this.provider.GetCount<T>(null, null, null, null);
            return this.provider.Query<T>(columns, null, null, null, null, null, null, pagingSkipCount, pagingTakeCount);
        }
        public IEnumerable<T> Query<T>(int pagingSkipCount, int pagingTakeCount, out int count) where T : new()
        {
            count = this.provider.GetCount<T>(null, null, null, null);
            return this.provider.Query<T>(null, null, null, null, null, null, pagingSkipCount, pagingTakeCount);
        }



        public T GetEntity<T>(string[] columns, Expression<Func<T, bool>> condition) where T : new()
        {            
            var enumerator = this.provider.Query<T>(columns, condition);
            foreach (T t in enumerator)
            {
                return t;
            }
            return default(T);
        }

        public T GetEntity<T>(string[] columns, Expression expression) where T : new()
        {            
            var enumerator = this.provider.Query<T>(columns, expression);
            foreach (T t in enumerator)
            {
                return t;
            }
            return default(T);
        }
                
        public T GetEntity<T>(Expression<Func<T, bool>> condition) where T : new()
        {
            var enumerator = this.provider.Query<T>(null, condition);
            foreach (T t in enumerator)
            {
                return t;
            }
            return default(T);
        }
        
        public T GetEntity<T>(Expression expression) where T : new()
        {
            var enumerator = this.provider.Query<T>(null, expression);
            foreach (T t in enumerator)
            {
                return t;
            }
            return default(T);
        }


        public List<T> QueryForList<T>(Expression<Func<T, object>>[] columnExpressions, Expression<Func<T, bool>> condition) where T : new()
        {
            string[] columns = GetColumns<T>(columnExpressions);
            return this.provider.Query<T>(columns, condition).ToList();
        }

        public List<T> QueryForList<T>(string[] columns, Expression<Func<T, bool>> condition) where T : new()
        {
            return this.provider.Query<T>(columns, condition).ToList();
        }

        public List<T> QueryForList<T>(string[] columns, Expression expression) where T : new()
        {
            return this.provider.Query<T>(columns, expression).ToList();
        }

        public List<T> QueryForList<T>(Expression<Func<T, bool>> condition) where T : new()
        {
            return this.provider.Query<T>(null, condition).ToList();
        }

        public List<T> QueryForList<T>(Expression expression) where T : new()
        {
            return this.provider.Query<T>(null, expression).ToList();
        }

        public List<T> QueryForList<T>() where T : new()
        {
            return this.provider.Query<T>(null, null).ToList();
        }
    }
}
