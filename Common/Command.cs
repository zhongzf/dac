using System;
using System.Collections.Generic;
using System.Text;
using System.ComponentModel;
using System.Data;

namespace RaisingStudio.Data.Common
{
    /// <summary>
    /// Command.
    /// </summary>
    [Serializable]
    public class Command
    {
        private string commandText = string.Empty;
        /// <summary>
        /// command text.
        /// </summary>
        [DefaultValue("")]
        public string CommandText
        {
            get
            {
                return this.commandText;
            }
            set
            {
                this.commandText = value;
            }
        }

        protected ParameterCollection parameters;
        /// <summary>
        ///Parameter.
        /// </summary>
        public ParameterCollection Parameters
        {
            get
            {
                if (this.parameters == null)
                {
                    this.parameters = new ParameterCollection();
                }
                return this.parameters;
            }
        }

        private System.Data.CommandType commandType = System.Data.CommandType.Text;
        /// <summary>
        /// Indicates or specifies how the CommandText property is interpreted.
        /// </summary>
        [DefaultValue(System.Data.CommandType.Text)]
        public System.Data.CommandType CommandType
        {
            get
            {
                return this.commandType;
            }
            set
            {
                this.commandType = value;
            }
        }

        private int commandTimeout = 30;
        /// <summary>
        /// Gets or sets the wait time before terminating the attempt to execute a command and generating an error.
        /// </summary>
        [DefaultValue(30)]
        public int CommandTimeout
        {
            get
            {
                return this.commandTimeout;
            }
            set
            {
                this.commandTimeout = value;
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public Command()
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public Command(string commandText) : this()
        {
            this.commandText = commandText;
        }

        public Command(string commandText, string[] parameterNames, object[] parameterValues) :this(commandText)
        {
            for (int i = 0; i < parameterNames.Length; i++)
            {
                AddParameter(parameterNames[i], parameterValues[i]);
            }
        }

        /// <summary>
        /// Return the command text of command.
        /// </summary>
        /// <returns>The command text of command.</returns>
        public override string ToString()
        {
            return this.commandText;
        }

        public bool HasParameters
        {
            get
            {
                return ((this.parameters != null) && (this.parameters.Count > 0));
            }
        }

        private string _parameterPrefix = "@";
        [DefaultValue("@")]
        public string ParameterPrefix
        {
            get
            {
                return this._parameterPrefix;
            }
            set
            {
                this._parameterPrefix = value;
            }
        }

        private bool _useParameterPrefixInSql = true;
        [DefaultValue(true)]
        public bool UseParameterPrefixInSql
        {
            get
            {
                return this._useParameterPrefixInSql;
            }
            set
            {
                this._useParameterPrefixInSql = value;
            }
        }

        private bool _useParameterPrefixInParameter = true;
        [DefaultValue(true)]
        public bool UseParameterPrefixInParameter
        {
            get
            {
                return this._useParameterPrefixInParameter;
            }
            set
            {
                this._useParameterPrefixInParameter = value;
            }
        }

        private bool _useBrackets = true;
        [DefaultValue(true)]
        public bool UseBrackets
        {
            get
            {
                return this._useBrackets;
            }
            set
            {
                this._useBrackets = value;
            }
        }


        public Parameter AddParameter(string parameterName)
        {
            return this.Parameters.Add(parameterName);
        }

        public Parameter AddParameter(string parameterName, object value)
        {
            return this.Parameters.Add(parameterName, value);
        }

        public Parameter AddParameter(string parameterName, DbType dbType)
        {
            return this.Parameters.Add(parameterName, dbType);
        }

        public Parameter AddParameter(string parameterName, DbType dbType, object value)
        {
            return this.Parameters.Add(parameterName, dbType, value);
        }
    }
}
