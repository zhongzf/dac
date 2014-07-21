using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.IO;
using System.Diagnostics;

namespace RaisingStudio.Data.Common
{
    public class Database : IDatabase
    {
        private IDbConnection connection;
        public IDbConnection Connection
        {
            get
            {
                return this.connection;
            }
        }


        private string providerName;
        public string ProviderName
        {
            get
            {
                return this.providerName;
            }
        }


        private CommandConverter commandConverter;

        public Database(IDbConnection connection, string providerName)
        {
            this.connection = connection;
            this.providerName = providerName;
            this.commandConverter = new CommandConverter(this.connection, this.providerName); 
        }

        #region Log
        public TextWriter Log { get; set; }

        public virtual void WriteLog(string value)
        {
            if (this.Log != null)
            {
                this.Log.WriteLine(value);
            }
        }
        public virtual void WriteLog(string format, params object[] args)
        {
            if (this.Log != null)
            {
                this.Log.WriteLine(format, args);
            }
        }
        public virtual void WriteLog(IDbCommand command)
        {
            if (this.Log != null)
            {
                string commandText = command.CommandText;
                this.Log.WriteLine(commandText);
                foreach (System.Data.IDbDataParameter parameter in command.Parameters)
                {
                    // sample, -- @p0: Input NVarChar (Size = 6; Prec = 0; Scale = 0) [London]
                    this.Log.WriteLine("-- {0}: {1} {2} (Size = {3}; Prec = {4}, Scale = {5}) [{6}]", parameter.ParameterName, parameter.Direction, parameter.DbType, parameter.Size, parameter.Precision, parameter.Scale, parameter.Value);
                }
                //sample, -- Context: OTHER, oracle.10.1
                this.Log.WriteLine("-- Context: {0}, {1}", command.GetType(), command.GetType().Assembly);
                this.Log.WriteLine();
            }
        }
        #endregion

        #region Transaction
        public IDbTransaction Transaction { get; set; }

        protected ConnectionState previousConnectionState;
        /// <summary>
        /// connection state.
        /// </summary>
        public ConnectionState PreviousConnectionState
        {
            get
            {
                return this.previousConnectionState;
            }
        }

        protected bool connectionToBeClosed = false;
        /// <summary>
        /// close the connection or not.
        /// </summary>
        public bool ConnectionToBeClosed
        {
            get
            {
                return this.connectionToBeClosed;
            }
        }

        /// <summary>
        /// Begin Database transaction.
        /// </summary>
        /// <returns>The ID of transaction.</returns>
        public virtual IDbTransaction BeginTransaction()
        {
            try
            {
                this.previousConnectionState = this.connection.State;
                if ((this.connection.State & System.Data.ConnectionState.Open) != System.Data.ConnectionState.Open)
                {
                    this.connection.Open();
                    this.connectionToBeClosed = true;
                }
                IDbTransaction transaction = this.connection.BeginTransaction();
                this.Transaction = transaction;
                return transaction;
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
                throw;
            }
        }

        /// <summary>
        /// Begin Database transaction.
        /// </summary>
        /// <param name="isolationLevel">Specifies the isolation level for the transaction.</param>
        /// <returns>The ID of transaction.</returns>
        public virtual IDbTransaction BeginTransaction(IsolationLevel isolationLevel)
        {
            try
            {
                this.previousConnectionState = this.connection.State;
                if ((this.connection.State & System.Data.ConnectionState.Open) != System.Data.ConnectionState.Open)
                {
                    this.connection.Open();
                    this.connectionToBeClosed = true;
                }
                IDbTransaction transaction = this.connection.BeginTransaction(isolationLevel);
                this.Transaction = transaction;
                return transaction;
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex);
                throw;
            }
        }

        /// <summary>
        /// Commit Database transaction.
        /// </summary>
        public virtual void CommitTransaction()
        {
            if (this.Transaction != null)
            {
                try
                {
                    this.Transaction.Commit();
                }
                finally
                {
                    this.Transaction = null;
                    if (this.connectionToBeClosed)
                    {
                        this.connection.Close();
                    }
                }
            }
            else
            {
                throw new System.InvalidOperationException("Please begin the transaction first..");
            }
        }

        /// <summary>
        /// Rollback transaction.
        /// </summary>
        public virtual void RollbackTransaction()
        {
            if (this.Transaction != null)
            {
                try
                {
                    this.Transaction.Rollback();
                }
                finally
                {
                    this.Transaction = null;
                    if (this.connectionToBeClosed)
                    {
                        this.connection.Close();
                    }
                }
            }
            else
            {
                throw new System.InvalidOperationException("Please begin the transaction first..");
            }
        }
        #endregion


        #region IDatabase Members

        public int ExecuteNonQuery(Command command)
        {
            IDbCommand dbCommand = this.commandConverter.Convert(command);
            var result = this.ExecuteNonQuery(dbCommand);
            this.commandConverter.FeedbackParameters(ref command, dbCommand);
            return result;
        }

        public object ExecuteScalar(Command command)
        {
            IDbCommand dbCommand = this.commandConverter.Convert(command);
            var result = this.ExecuteScalar(dbCommand);
            this.commandConverter.FeedbackParameters(ref command, dbCommand);
            return result;
        }

        public IDataReader ExecuteReader(Command command)
        {
            IDbCommand dbCommand = this.commandConverter.Convert(command);
            var result = this.ExecuteReader(dbCommand);
            this.commandConverter.FeedbackParameters(ref command, dbCommand);
            return result;
        }

        #endregion


        public int ExecuteNonQuery(IDbCommand command)
        {
            WriteLog(command);
            bool closeConnection = false;
            System.Data.ConnectionState previousConnectionState = command.Connection.State;
            if ((command.Connection.State & System.Data.ConnectionState.Open) != System.Data.ConnectionState.Open)
            {
                command.Connection.Open();
                closeConnection = true;
            }
            try
            {
                command.Transaction = this.Transaction;
                int returnValue = command.ExecuteNonQuery();
                return returnValue;
            }
            finally
            {
                if (closeConnection)
                {
                    command.Connection.Close();
                }
            }
        }

        public object ExecuteScalar(IDbCommand command)
        {
            WriteLog(command); 
            bool closeConnection = false;
            System.Data.ConnectionState previousConnectionState = command.Connection.State;
            if ((command.Connection.State & System.Data.ConnectionState.Open) != System.Data.ConnectionState.Open)
            {
                command.Connection.Open();
                closeConnection = true;
            }
            try
            {
                command.Transaction = this.Transaction; 
                object returnValue = command.ExecuteScalar();
                return returnValue;
            }
            finally
            {
                if (closeConnection)
                {
                    command.Connection.Close();
                }
            }
        }

        public IDataReader ExecuteReader(IDbCommand command)
        {
            WriteLog(command); 
            bool closeConnection = false;
            System.Data.ConnectionState previousConnectionState = command.Connection.State;
            if ((command.Connection.State & System.Data.ConnectionState.Open) != System.Data.ConnectionState.Open)
            {
                command.Connection.Open();
                closeConnection = true;
            }
            try
            {
                command.Transaction = this.Transaction; 
                if (previousConnectionState == System.Data.ConnectionState.Closed)
                {
                    System.Data.IDataReader returnValue = command.ExecuteReader(System.Data.CommandBehavior.CloseConnection);
                    return returnValue;
                }
                else
                {
                    System.Data.IDataReader returnValue = command.ExecuteReader();
                    return returnValue;
                }
            }
            catch
            {
                if (closeConnection)
                {
                    command.Connection.Close();
                }
                throw;
            }
        }


        public int ExecuteNonQuery(string commandText)
        {
            return this.ExecuteNonQuery(this.commandConverter.Convert(commandText));
        }

        public object ExecuteScalar(string commandText)
        {
            return this.ExecuteScalar(this.commandConverter.Convert(commandText));
        }

        public IDataReader ExecuteReader(string commandText)
        {
            return this.ExecuteReader(this.commandConverter.Convert(commandText));
        }
    }
}
