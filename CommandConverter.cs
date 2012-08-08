using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using RaisingStudio.Data.Common;

namespace RaisingStudio.Data
{
    public class CommandConverter
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

        private ParameterDirection direction = ParameterDirection.Input;
        private DbType dbType = DbType.String;
        private DataRowVersion sourceVersion = DataRowVersion.Default;

        public const string POSITIONALPARAMETER = "?";

        private bool _usePositionalParameters = false;
        private string _parameterPrefix = "@";
        private bool _useParameterPrefixInParameter = true;
        private bool _useParameterPrefixInSql = true;

        public const string CONFIGURATION_COMMAND_CONVERTER_SECTION_NAME = "raisingstudio.data/CommandConverter.Settings";

        public CommandConverter(IDbConnection connection, string providerName)
        {
            this.connection = connection;
            this.providerName = providerName;

            InitDefaultValues();
        }

        private void InitDefaultValues()
        {
            Type parameterType = typeof(Parameter);
            object directionDefaultValue = GetDefalutValue(parameterType, "Direction");
            if (directionDefaultValue is ParameterDirection)
            {
                this.direction = (ParameterDirection)directionDefaultValue;
            }
            object dbTypeDefaultValue = GetDefalutValue(parameterType, "DbType");
            if (dbTypeDefaultValue is DbType)
            {
                this.dbType = (DbType)dbTypeDefaultValue;
            }
            object sourceVersionDefaultValue = GetDefalutValue(parameterType, "SourceVersion");
            if (sourceVersionDefaultValue is DataRowVersion)
            {
                this.sourceVersion = (DataRowVersion)sourceVersionDefaultValue;
            }

            if (this.connection is SqlConnection)
            {
                _usePositionalParameters = false;
                _parameterPrefix = "@";
                _useParameterPrefixInParameter = true;
                _useParameterPrefixInSql = true;
            }
            else if (this.connection is OdbcConnection)
            {
                _usePositionalParameters = true;
                _parameterPrefix = "";
                _useParameterPrefixInParameter = false;
                _useParameterPrefixInSql = false;
            }
            else if (this.connection is OleDbConnection)
            {
                _usePositionalParameters = true;
                _parameterPrefix = "";
                _useParameterPrefixInParameter = false;
                _useParameterPrefixInSql = false;
            }
            else
            {
                ProcessSettings(CONFIGURATION_COMMAND_CONVERTER_SECTION_NAME);
            }
        }

        private object GetDefalutValue(Type parameterType, string propertyName)
        {
            PropertyInfo propertyInfo = parameterType.GetProperty(propertyName);
            var customAttributes = propertyInfo.GetCustomAttributes(typeof(DefaultValueAttribute), true);
            if ((customAttributes != null) && (customAttributes.Length > 0))
            {
                DefaultValueAttribute defaultValueAttribute = customAttributes[0] as DefaultValueAttribute;
                if (defaultValueAttribute != null)
                {
                    return defaultValueAttribute.Value;
                }
            }
            return null;
        }

        private void ProcessSettings(string sectionName)
        {
            CommandConverterSettings commandConverterSettings = (CommandConverterSettings)ConfigurationManager.GetSection(sectionName);
            if (commandConverterSettings != null)
            {
                for (int i = 0; i < commandConverterSettings.Values.Count; i++)
                {
                    CommandConverterSetting commandConverterSetting = commandConverterSettings.Values[i];
                    string providerName = commandConverterSetting.ProviderName;
                    if (providerName == this.providerName)
                    {
                        try
                        {
                            _usePositionalParameters = commandConverterSetting.UsePositionalParameters;
                            _parameterPrefix = commandConverterSetting.ParameterPrefix;
                            _useParameterPrefixInParameter = commandConverterSetting.UseParameterPrefixInParameter;
                            _useParameterPrefixInSql = commandConverterSetting.UseParameterPrefixInSql;
                            break;
                        }
                        catch (Exception ex)
                        {
                            Debug.WriteLine(ex);
                        }
                    }
                }
            }
        }

        public IDbCommand Convert(string commandText)
        {
            IDbCommand dbCommand = this.connection.CreateCommand();
            dbCommand.CommandText = commandText;
            return dbCommand;
        }

        public IDbCommand Convert(Command commonCommand)
        {
            if (commonCommand != null)
            {
                IDbCommand dbCommand = this.connection.CreateCommand();
                dbCommand.CommandType = commonCommand.CommandType;
                string commandText = commonCommand.CommandText;
                foreach (Parameter commonParameter in commonCommand.Parameters)
                {
                    string parameterName = ReplaceParameterName(ref commandText, commonParameter.ParameterName, commonCommand.ParameterPrefix, commonCommand.UseParameterPrefixInParameter, commonCommand.UseParameterPrefixInSql);
                    System.Data.IDbDataParameter parameter = dbCommand.CreateParameter();
                    if (commonParameter.Direction != this.direction)
                    {
                        parameter.Direction = commonParameter.Direction;
                    }
                    if (commonParameter.DbType != this.dbType)
                    {
                        parameter.DbType = commonParameter.DbType;
                        parameter.Size = commonParameter.Size;
                        parameter.Scale = commonParameter.Scale;
                        parameter.Precision = commonParameter.Precision;
                    }
                    if (commonParameter.SourceVersion != this.sourceVersion)
                    {
                        parameter.SourceVersion = commonParameter.SourceVersion;
                    }
                    parameter.ParameterName = parameterName;
                    object value = commonParameter.Value;
                    if (value == null)
                    {
                        parameter.Value = System.DBNull.Value;
                    }
                    else
                    {
                        parameter.Value = value;
                    }
                    dbCommand.Parameters.Add(parameter);
                }
                dbCommand.CommandText = commandText;
                return dbCommand;
            }
            return null;
        }

        public static string TrimStart(string content, string value)
        {
            if (content.StartsWith(value))
            {
                return content.Substring(value.Length, content.Length - value.Length);
            }
            return content;
        }

        private string ReplaceParameterName(ref string commandText, string parameterName, string parameterPrefix, bool useParameterPrefixInParameter, bool useParameterPrefixInSql)
        {
            string pureParameterName = useParameterPrefixInParameter ? TrimStart(parameterName, parameterPrefix) : parameterName;
            string parameterNameInSql = useParameterPrefixInSql ? (useParameterPrefixInParameter ? parameterName : parameterPrefix + pureParameterName) : pureParameterName;
            string _parameterNameInParameter = _useParameterPrefixInParameter ? _parameterPrefix + pureParameterName : pureParameterName;
            string _parameterNameInSql = _usePositionalParameters ? POSITIONALPARAMETER : (_useParameterPrefixInSql ? _parameterPrefix + pureParameterName : pureParameterName);
            if (parameterNameInSql != _parameterNameInSql)
            {
                commandText = commandText.Replace(parameterNameInSql, _parameterNameInSql);
            }
            return _parameterNameInParameter;
        }

        public void FeedbackParameters(ref Command commonCommand, System.Data.IDbCommand command)
        {
            if (command.Parameters != null)
            {
                for (int i = 0; i < command.Parameters.Count; i++)
                {
                    System.Data.IDbDataParameter parameter = command.Parameters[i] as System.Data.IDbDataParameter;
                    if (parameter.Direction != System.Data.ParameterDirection.Input)
                    {
                        commonCommand.Parameters[i].Value = parameter.Value;
                    }
                }
            }
        }
    }
}
