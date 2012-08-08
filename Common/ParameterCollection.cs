using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace RaisingStudio.Data.Common
{
    [Serializable]
    public class ParameterCollection : List<Parameter>
    {
        /// <summary>
        /// Add parameter.
        /// </summary>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="value">Parameter value.</param>
        public Parameter Add(string parameterName)
        {
            Parameter commonParameter = new Parameter(parameterName);
            this.Add(commonParameter);
            return commonParameter;
        }

        /// <summary>
        /// Add parameter.
        /// </summary>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="value">Parameter value.</param>
        public Parameter Add(string parameterName, object value)
        {
            Parameter commonParameter = new Parameter(parameterName, value);
            this.Add(commonParameter);
            return commonParameter;
        }

        /// <summary>
        /// Add parameter.
        /// </summary>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="dbType">Parameter Data type.</param>
        public Parameter Add(string parameterName, System.Data.DbType dbType)
        {
            Parameter commonParameter = new Parameter(parameterName, dbType);
            this.Add(commonParameter);
            return commonParameter;
        }

        /// <summary>
        /// Add parameter.
        /// </summary>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="dbType">Parameter Data type.</param>
        /// <param name="value">Parameter value.</param>
        public Parameter Add(string parameterName, System.Data.DbType dbType, object value)
        {
            Parameter commonParameter = new Parameter(parameterName, dbType, value);
            this.Add(commonParameter);
            return commonParameter;
        }

        /// <summary>
        /// Add parameter.
        /// </summary>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="dbType">Parameter Data type.</param>
        /// <param name="size">Parameter size.</param>
        /// <param name="sourceColumn">Source column name.</param>
        public Parameter Add(string parameterName, System.Data.DbType dbType, int size, string sourceColumn)
        {
            Parameter commonParameter = new Parameter(parameterName, dbType, size, sourceColumn);
            this.Add(commonParameter);
            return commonParameter;
        }

        /// <summary>
        /// Add parameter.
        /// </summary>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="direction">Gets or sets a value that indicates whether the parameter is input-only, output-only, bidirectional, or a stored procedure return value parameter.</param>
        /// <param name="sourceColumn">Source column name.</param>
        /// <param name="sourceVersion">Gets or sets the DataRowVersion to use when you load Value. </param>
        /// <param name="sourceColumnNullMapping">Sets or gets a value which indicates whether the source column is nullable. This allows DbCommandBuilder to correctly generate Update statements for nullable columns.</param>
        public Parameter Add(string parameterName, ParameterDirection direction, string sourceColumn, DataRowVersion sourceVersion, bool sourceColumnNullMapping)
        {
            Parameter commonParameter = new Parameter(parameterName, direction, sourceColumn, sourceVersion, sourceColumnNullMapping);
            this.Add(commonParameter);
            return commonParameter;
        }

        /// <summary>
        /// Add parameter.
        /// </summary>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="dbType">Parameter Data type.</param>
        /// <param name="direction">Gets or sets a value that indicates whether the parameter is input-only, output-only, bidirectional, or a stored procedure return value parameter.</param>
        /// <param name="value">Parameter value.</param>
        public Parameter Add(string parameterName, System.Data.DbType dbType, ParameterDirection direction, object value)
        {
            Parameter commonParameter = new Parameter(parameterName, dbType, direction, value);
            this.Add(commonParameter);
            return commonParameter;
        }

        /// <summary>
        /// Add parameter.
        /// </summary>
        /// <param name="parameterName">Parameter name.</param>
        /// <param name="dbType">Parameter Data type.</param>
        /// <param name="direction">Gets or sets a value that indicates whether the parameter is input-only, output-only, bidirectional, or a stored procedure return value parameter.</param>
        /// <param name="sourceColumn">Source column name.</param>
        /// <param name="sourceVersion">Gets or sets the DataRowVersion to use when you load Value. </param>
        /// <param name="sourceColumnNullMapping">Sets or gets a value which indicates whether the source column is nullable. This allows DbCommandBuilder to correctly generate Update statements for nullable columns.</param>
        public Parameter Add(string parameterName, System.Data.DbType dbType, ParameterDirection direction, string sourceColumn, DataRowVersion sourceVersion, bool sourceColumnNullMapping)
        {
            Parameter commonParameter = new Parameter(parameterName, dbType, direction, sourceColumn, sourceVersion, sourceColumnNullMapping);
            this.Add(commonParameter);
            return commonParameter;
        }
    }
}
