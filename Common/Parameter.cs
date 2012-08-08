using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.ComponentModel;

namespace RaisingStudio.Data.Common
{
    /// <summary>
    /// Parameter.
    /// </summary>
    [Serializable]
    public class Parameter
    {
        private System.Data.ParameterDirection direction = ParameterDirection.Input;
        /// <summary>
        /// Gets or sets a value, Gets or sets a value that indicates whether the parameter is input-only, output-only, bidirectional, or a stored procedure return value parameter.
        /// </summary>
        [DefaultValue(ParameterDirection.Input)]
        public System.Data.ParameterDirection Direction
        {
            get
            {
                return this.direction;
            }
            set
            {
                this.direction = value;
            }
        }

        private System.Data.DbType dbType = DbType.String;
        /// <summary>
        /// Gets or sets DbType。
        /// </summary>
        [DefaultValue(DbType.String)]
        public System.Data.DbType DbType
        {
            get
            {
                return this.dbType;
            }
            set
            {
                this.dbType = value;
            }
        }

        private object value;
        /// <summary>
        /// Gets or sets the Parameter value.
        /// </summary>
        public object Value
        {
            get
            {
                return this.value; ;
            }
            set
            {
                if (value == null)
                {
                    this.value = System.DBNull.Value;
                }
                else
                {
                    this.value = value;
                }
            }
        }

        private bool nullable = true;
        /// <summary>
        /// Gets or sets a value that indicates whether the parameter accepts null values. 
        /// </summary>
        public bool IsNullable
        {
            get
            {
                return this.nullable;
            }
            set
            {
                this.nullable = value;
            }
        }

        private System.Data.DataRowVersion sourceVersion = DataRowVersion.Default;
        /// <summary>
        /// Gets or sets the DataRowVersion to use when you load Value. 
        /// </summary>
        [DefaultValue(DataRowVersion.Default)]
        public System.Data.DataRowVersion SourceVersion
        {
            get
            {
                return this.sourceVersion;
            }
            set
            {
                this.sourceVersion = value;
            }
        }

        private string parameterName;
        /// <summary>
        /// Gets or sets name of parameter.
        /// </summary>
        public string ParameterName
        {
            get
            {
                return this.parameterName;
            }
            set
            {
                this.parameterName = value;
            }
        }

        private string sourceColumn;
        /// <summary>
        /// Gets or sets the name of the source column mapped to the DataSet and used for loading or returning the Value. 
        /// </summary>
        public string SourceColumn
        {
            get
            {
                return this.sourceColumn;
            }
            set
            {
                this.sourceColumn = value;
            }
        }

        private bool sourceColumnNullMapping = true;
        /// <summary>
        /// Sets or gets a value which indicates whether the source column is nullable. This allows DbCommandBuilder to correctly generate Update statements for nullable columns. 
        /// </summary>
        public bool SourceColumnNullMapping
        {
            get
            {
                return this.sourceColumnNullMapping;
            }
            set
            {
                this.sourceColumnNullMapping = value;
            }
        }

        private byte precision;
        /// <summary>
        /// Indicates the precision of numeric parameters. 
        /// </summary>
        public byte Precision
        {
            get
            {
                return this.precision;
            }
            set
            {
                this.precision = value;
            }
        }

        private byte scale;
        /// <summary>
        /// Indicates the scale of numeric parameters. 
        /// </summary>
        public byte Scale
        {
            get
            {
                return this.scale;
            }
            set
            {
                this.scale = value;
            }
        }

        private int size;
        /// <summary>
        /// Parameter size.
        /// </summary>
        public int Size
        {
            get
            {
                return this.size;
            }
            set
            {
                this.size = value;
            }
        }

        #region Constructor
        /// <summary>
        /// Constructor
        /// </summary>
        public Parameter()
        {
        }


        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="value">Parameter value.</param>
        public Parameter(string parameterName)
            : this()
        {
            this.ParameterName = parameterName;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="value">Parameter value.</param>
        public Parameter(string parameterName, object value)
            : this()
        {
            this.ParameterName = parameterName;
            this.Value = value;
        }
        
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="dbType">Parameter Data type.</param>
        public Parameter(string parameterName, System.Data.DbType dbType)
            : this()
        {
            this.ParameterName = parameterName;
            this.DbType = dbType;
        }


        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="dbType">Parameter Data type.</param>
        /// <param name="value">Parameter value.</param>
        public Parameter(string parameterName, System.Data.DbType dbType, object value)
            : this()
        {
            this.ParameterName = parameterName;
            this.DbType = dbType;
            this.Value = value;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="dbType">Parameter Data type.</param>
        /// <param name="size">Parameter size.</param>
        /// <param name="sourceColumn">Source column name.</param>
        public Parameter(string parameterName, System.Data.DbType dbType, int size, string sourceColumn)
            : this()
        {
            this.ParameterName = parameterName;
            this.DbType = dbType;
            this.Size = size;
            this.SourceColumn = sourceColumn;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="direction">Gets or sets a value that indicates whether the parameter is input-only, output-only, bidirectional, or a stored procedure return value parameter.</param>
        /// <param name="sourceColumn">Source column name.</param>
        /// <param name="sourceVersion">Gets or sets the DataRowVersion to use when you load Value. </param>
        /// <param name="sourceColumnNullMapping">Sets or gets a value which indicates whether the source column is nullable. This allows DbCommandBuilder to correctly generate Update statements for nullable columns.</param>
        public Parameter(string parameterName, ParameterDirection direction, string sourceColumn, DataRowVersion sourceVersion, bool sourceColumnNullMapping)
            : this()
        {
            this.ParameterName = parameterName;
            this.Direction = direction;
            this.SourceColumn = sourceColumn;
            this.SourceVersion = sourceVersion;
            this.SourceColumnNullMapping = sourceColumnNullMapping;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="dbType">Parameter Data type.</param>
        /// <param name="direction">Gets or sets a value that indicates whether the parameter is input-only, output-only, bidirectional, or a stored procedure return value parameter.</param>
        /// <param name="value">Parameter value.</param>
        public Parameter(string parameterName, System.Data.DbType dbType, ParameterDirection direction, object value)
            : this()
        {
            this.ParameterName = parameterName;
            this.DbType = dbType;
            this.Direction = direction;
            this.Value = value;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="dbType">Parameter Data type.</param>
        /// <param name="direction">Gets or sets a value that indicates whether the parameter is input-only, output-only, bidirectional, or a stored procedure return value parameter.</param>
        /// <param name="sourceColumn">Source column name.</param>
        /// <param name="sourceVersion">Gets or sets the DataRowVersion to use when you load Value. </param>
        /// <param name="sourceColumnNullMapping">Sets or gets a value which indicates whether the source column is nullable. This allows DbCommandBuilder to correctly generate Update statements for nullable columns.</param>
        public Parameter(string parameterName, System.Data.DbType dbType, ParameterDirection direction, string sourceColumn, DataRowVersion sourceVersion, bool sourceColumnNullMapping)
            : this()
        {
            this.ParameterName = parameterName;
            this.DbType = dbType;
            this.Direction = direction;
            this.SourceColumn = sourceColumn;
            this.SourceVersion = sourceVersion;
            this.SourceColumnNullMapping = sourceColumnNullMapping;
        }
        #endregion
    }
}
