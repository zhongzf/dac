using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using RaisingStudio.Data.Converters;
using System.Configuration;
using System.Diagnostics;
using RaisingStudio.Data.Settings;

namespace RaisingStudio.Data
{
    public sealed class ConverterManager
    {
        #region Default Instance
        private static volatile ConverterManager _default;
        private static object default_lock = new object();
        public static ConverterManager Default
        {
            get
            {
                if (_default == null)
                {
                    lock (default_lock)
                    {
                        if (_default == null)
                        {
                            _default = new ConverterManager();
                        }
                    }
                }
                return _default;
            }
        }
        #endregion


        public static object ConvertToBool(object value)
        {
            return Convert.ToBoolean(value);
        }
        public static object ConvertBoolToInt16(object value)
        {
            return Convert.ToInt32(value);
        }
        public static object ConvertEnumToInt16(object value)
        {
            return (int)value;
        }
        public static object ConvertBoolToInt32(object value)
        {
            return Convert.ToInt32(value);
        }
        public static object ConvertEnumToInt32(object value)
        {
            return (int)value;
        }
        public static object ConvertBoolToInt64(object value)
        {
            return Convert.ToInt32(value);
        }
        public static object ConvertEnumToInt64(object value)
        {
            return (int)value;
        }

        public static object ConvertToInt16(object value)
        {
            return Convert.ToInt16(value);
        }
        public static object ConvertToInt32(object value)
        {
            return Convert.ToInt32(value);
        }
        public static object ConvertToInt64(object value)
        {
            return Convert.ToInt32(value);
        }
        public static object ConvertToDecimal(object value)
        {
            return Convert.ToDecimal(value);
        }
        public static object ConvertToDateTime(object value)
        {
            return Convert.ToDateTime(value);
        }
        public static object ConvertToSingle(object value)
        {
            return Convert.ToSingle(value);
        }
        public static object ConvertToDouble(object value)
        {
            return Convert.ToDouble(value);
        }
        public static object ConvertToString(object value)
        {
            return Convert.ToString(value);
        }
        public static object ConvertToGuid(object value)
        {
            if (value is string)
            {
                return new Guid((string)value);
            }
            else if (value is byte[])
            {
                return new Guid((byte[])value);
            }
            return value;
        }


        private Func<object, object> convertToBool;
        private Func<object, object> convertBoolToInt16;
        private Func<object, object> convertEnumToInt16;
        private Func<object, object> convertBoolToInt32;
        private Func<object, object> convertEnumToInt32;
        private Func<object, object> convertBoolToInt64;
        private Func<object, object> convertEnumToInt64;

        private Func<object, object> convertToInt16;
        private Func<object, object> convertToInt32;
        private Func<object, object> convertToInt64;
        private Func<object, object> convertToDecimal;
        private Func<object, object> convertToDateTime;
        private Func<object, object> convertToSingle;
        private Func<object, object> convertToDouble;
        private Func<object, object> convertToString;
        private Func<object, object> convertToGuid;

        public const string CONFIGURATION_DBTYPE_CONVERTER_SECTION_NAME = "raisingstudio.data/DbTypeConverter.Settings";

        private Dictionary<System.Data.DbType, Dictionary<System.Type, Func<object, object>>> _fromDbTypeDataConverters;
        private Dictionary<System.Data.DbType, Dictionary<System.Type, Func<object, object>>> _toDbTypeDataConverters;

        private ConverterManager()
        {
            this.convertToBool = new Func<object, object>(ConvertToBool);
            this.convertBoolToInt16 = new Func<object, object>(ConvertBoolToInt16);
            this.convertEnumToInt16 = new Func<object, object>(ConvertEnumToInt16);
            this.convertBoolToInt32 = new Func<object, object>(ConvertBoolToInt32);
            this.convertEnumToInt32 = new Func<object, object>(ConvertEnumToInt32);
            this.convertBoolToInt64 = new Func<object, object>(ConvertBoolToInt64);
            this.convertEnumToInt64 = new Func<object, object>(ConvertEnumToInt64);

            this.convertToInt16 = new Func<object, object>(ConvertToInt16);
            this.convertToInt32 = new Func<object, object>(ConvertToInt32);
            this.convertToInt64 = new Func<object, object>(ConvertToInt64);
            this.convertToDecimal = new Func<object, object>(ConvertToDecimal);
            this.convertToDateTime = new Func<object, object>(ConvertToDateTime);
            this.convertToSingle = new Func<object, object>(ConvertToSingle);
            this.convertToDouble = new Func<object, object>(ConvertToDouble);
            this.convertToString = new Func<object, object>(ConvertToString);
            this.convertToGuid = new Func<object, object>(ConvertToGuid);

            LoadDbTypeConverterSettings(CONFIGURATION_DBTYPE_CONVERTER_SECTION_NAME);
        }

        private void LoadDbTypeConverterSettings(string sectionName)
        {
            this._fromDbTypeDataConverters = new Dictionary<DbType, Dictionary<Type, Func<object, object>>>();
            this._toDbTypeDataConverters = new Dictionary<DbType, Dictionary<Type, Func<object, object>>>();
            DbTypeConverterSettings dbTypeConverterSettings = (DbTypeConverterSettings)ConfigurationManager.GetSection(sectionName);
            if (dbTypeConverterSettings != null)
            {
                for (int i = 0; i < dbTypeConverterSettings.Values.Count; i++)
                {
                    DbTypeConverterSetting dbTypeConverterSetting = dbTypeConverterSettings.Values[i];
                    if (dbTypeConverterSetting.Enabled)
                    {
                        try
                        {
                            IDbTypeConverter converter = Activator.CreateInstance(dbTypeConverterSetting.ConverterType) as IDbTypeConverter;
                            if (converter != null)
                            {
                                RegisterConverter(dbTypeConverterSetting.DbType, dbTypeConverterSetting.DataType, converter);
                            }
                        }
                        catch (Exception ex)
                        {
                            Debug.WriteLine(ex);
                        }
                    }
                }
            }
        }


        public void RegisterConverter(System.Data.DbType dbType, System.Type type, IDbTypeConverter converter)
        {
            if (!this._fromDbTypeDataConverters.ContainsKey(dbType))
            {
                this._fromDbTypeDataConverters.Add(dbType, new Dictionary<System.Type, Func<object, object>>());
            }
            Dictionary<System.Type, Func<object, object>> fromDataConverters = this._fromDbTypeDataConverters[dbType];
            fromDataConverters[type] = new Func<object, object>(converter.ConvertFromDbType);

            if (!this._toDbTypeDataConverters.ContainsKey(dbType))
            {
                this._toDbTypeDataConverters.Add(dbType, new Dictionary<System.Type, Func<object, object>>());
            }
            Dictionary<System.Type, Func<object, object>> toDataConverters = this._toDbTypeDataConverters[dbType];
            toDataConverters[type] = new Func<object, object>(converter.ConvertToDbType);
        }

        public Func<object, object> GetFromDbTypeDataConverter(System.Data.DbType dbType, System.Type type)
        {
            if (this._fromDbTypeDataConverters.ContainsKey(dbType))
            {
                Dictionary<System.Type, Func<object, object>> fromDataConverters = this._fromDbTypeDataConverters[dbType];
                if (fromDataConverters.ContainsKey(type))
                {
                    return fromDataConverters[type];
                }
            }
            return null;
        }

        public Func<object, object> GetToDbTypeDataConverter(System.Data.DbType dbType, System.Type type)
        {
            if (this._toDbTypeDataConverters.ContainsKey(dbType))
            {
                Dictionary<System.Type, Func<object, object>> toDataConverters = this._toDbTypeDataConverters[dbType];
                if (toDataConverters.ContainsKey(type))
                {
                    return toDataConverters[type];
                }
            }
            return null;
        }


        public Func<object, object> GetConverter(string columnType, Type type)
        {
            if (!string.IsNullOrWhiteSpace(columnType))
            {
                DbType dbType = TypeManager.GetWellKnownDbType(columnType);
                var converter = GetFromDbTypeDataConverter(dbType, type);
                if (converter != null)
                {
                    return converter;
                }
                #region
                if (type == typeof(int) || type == typeof(int?))
                {
                    if (dbType == DbType.Int64 || dbType == DbType.UInt64 || dbType == DbType.Int16 || dbType == DbType.UInt16 || dbType == DbType.String || dbType == DbType.Decimal || dbType == DbType.Single || dbType == DbType.Double || dbType == DbType.Currency || dbType == DbType.Boolean)
                    {
                        return this.convertToInt32;
                    }
                }
                if (type == typeof(short) || type == typeof(short?))
                {
                    if (dbType == DbType.Int64 || dbType == DbType.UInt64 || dbType == DbType.Int32 || dbType == DbType.UInt32 || dbType == DbType.String || dbType == DbType.Decimal || dbType == DbType.Single || dbType == DbType.Double || dbType == DbType.Currency || dbType == DbType.Boolean)
                    {
                        return this.convertToInt16;
                    }
                }
                if (type == typeof(long) || type == typeof(long?))
                {
                    if (dbType == DbType.Int16 || dbType == DbType.UInt16 || dbType == DbType.Int32 || dbType == DbType.UInt32 || dbType == DbType.String || dbType == DbType.Decimal || dbType == DbType.Single || dbType == DbType.Double || dbType == DbType.Currency || dbType == DbType.Boolean)
                    {
                        return this.convertToInt64;
                    }
                }
                if (type == typeof(decimal) || type == typeof(decimal?))
                {
                    if (dbType == DbType.String || dbType == DbType.Int16 || dbType == DbType.Int32 || dbType == DbType.Int64 || dbType == DbType.SByte || dbType == DbType.UInt16 || dbType == DbType.UInt32 || dbType == DbType.UInt64)
                    {
                        return this.convertToDecimal;
                    }
                }
                if (type == typeof(DateTime) || type == typeof(DateTime?))
                {
                    if (dbType == DbType.String || dbType == DbType.Int16 || dbType == DbType.Int32 || dbType == DbType.Int64 || dbType == DbType.UInt16 || dbType == DbType.UInt32 || dbType == DbType.UInt64)
                    {
                        return this.convertToDateTime;
                    }
                }
                if (type == typeof(float) || type == typeof(float?))
                {
                    if (dbType == DbType.String || dbType == DbType.Currency || dbType == DbType.Decimal || dbType == DbType.Int16 || dbType == DbType.Int32 || dbType == DbType.Int64 || dbType == DbType.SByte || dbType == DbType.UInt16 || dbType == DbType.UInt32 || dbType == DbType.UInt64)
                    {
                        return this.convertToSingle;
                    }
                }
                if (type == typeof(double) || type == typeof(double?))
                {
                    if (dbType == DbType.String || dbType == DbType.Currency || dbType == DbType.Decimal || dbType == DbType.Int16 || dbType == DbType.Int32 || dbType == DbType.Int64 || dbType == DbType.SByte || dbType == DbType.UInt16 || dbType == DbType.UInt32 || dbType == DbType.UInt64)
                    {
                        return this.convertToDouble;
                    }
                }
                if (type == typeof(string))
                {
                    if (dbType != DbType.String && dbType != DbType.StringFixedLength && dbType != DbType.AnsiString && dbType != DbType.AnsiStringFixedLength)
                    {
                        return this.convertToString;
                    }
                }
                #endregion
                if ((dbType == System.Data.DbType.Int32) || (dbType == System.Data.DbType.Int16) || (dbType == System.Data.DbType.Int64) || (dbType == System.Data.DbType.UInt32) || (dbType == System.Data.DbType.UInt16) || (dbType == System.Data.DbType.UInt64))
                {
                    #region Int32
                    if ((type == typeof(bool)) || (type == typeof(bool?)))
                    {
                        return this.convertToBool;
                    }
                    else if (type.IsEnum)
                    {
                        return new IntToEnumConverter(type).Convert;
                    }
                    else if (type.IsGenericType)
                    {
                        if (type.GetGenericTypeDefinition() == typeof(Nullable<>))
                        {
                            Type argumentsType = type.GetGenericArguments()[0];
                            if ((argumentsType == typeof(bool)) || (argumentsType == typeof(bool?)))
                            {
                                return this.convertToBool;
                            }
                            else if (argumentsType.IsEnum)
                            {
                                return new IntToEnumConverter(argumentsType).Convert;
                            }
                            else
                            {
                                // TODO:
                                return null;
                            }
                        }
                    }
                    else
                    {
                        // TODO:
                        return null;
                    }
                    #endregion
                }
                else if (dbType == System.Data.DbType.String)
                {
                    #region String
                    if ((type == typeof(bool)) || (type == typeof(bool?)))
                    {
                        return this.convertToBool;
                    }
                    else if (type.IsEnum)
                    {
                        return new EnumConverter(type).ConvertFromDbType;
                    }
                    else if (type.IsGenericType)
                    {
                        if (type.GetGenericTypeDefinition() == typeof(Nullable<>))
                        {
                            Type argumentsType = type.GetGenericArguments()[0];
                            if (argumentsType.IsEnum)
                            {
                                return new EnumConverter(argumentsType).ConvertFromDbType;
                            }
                            else
                            {
                                // TODO:
                                return null;
                            }
                        }
                    }
                    #endregion
                }
                else
                {
                    // TODO:
                    return null;
                }
            }
            else
            {
                return null;
            }
            return null;
        }

        public Func<object, object> GetConverter(Type type, string columnType)
        {
            if (!string.IsNullOrWhiteSpace(columnType))
            {
                DbType dbType = TypeManager.GetWellKnownDbType(columnType);
                return GetConverter(type, dbType);
            }
            return null;
        }

        public Func<object, object> GetConverter(Type type, DbType dbType)
        {
            var converter = GetToDbTypeDataConverter(dbType, type);
            if (converter != null)
            {
                return converter;
            }
            if (dbType == System.Data.DbType.Int32)
            {
                #region Int32
                if ((type == typeof(bool)) || (type == typeof(bool?)))
                {
                    return this.convertBoolToInt32;
                }
                else if (type.IsEnum)
                {
                    return this.convertEnumToInt32;
                }
                else if (type.IsGenericType)
                {
                    if (type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        Type argumentsType = type.GetGenericArguments()[0];
                        if (argumentsType.IsEnum)
                        {
                            return this.convertEnumToInt32;
                        }
                        else
                        {
                            // TODO:
                            return null;
                        }
                    }
                }
                else if (type == typeof(string))
                {
                    return this.convertToInt32;
                }
                else
                {
                    return null;
                }
                #endregion
            }
            else if (dbType == System.Data.DbType.Int16)
            {
                #region Int16
                if ((type == typeof(bool)) || (type == typeof(bool?)))
                {
                    return this.convertBoolToInt16;
                }
                else if (type.IsEnum)
                {
                    return this.convertEnumToInt16;
                }
                else if (type.IsGenericType)
                {
                    if (type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        Type argumentsType = type.GetGenericArguments()[0];
                        if (argumentsType.IsEnum)
                        {
                            return this.convertEnumToInt16;
                        }
                        else
                        {
                            // TODO:
                            return null;
                        }
                    }
                }
                else if (type == typeof(string))
                {
                    return this.convertToInt16;
                }
                else
                {
                    return null;
                }
                #endregion
            }
            else if (dbType == System.Data.DbType.Int64)
            {
                #region Int64
                if ((type == typeof(bool)) || (type == typeof(bool?)))
                {
                    return this.convertBoolToInt64;
                }
                else if (type.IsEnum)
                {
                    return this.convertEnumToInt64;
                }
                else if (type.IsGenericType)
                {
                    if (type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        Type argumentsType = type.GetGenericArguments()[0];
                        if (argumentsType.IsEnum)
                        {
                            return this.convertEnumToInt64;
                        }
                        else
                        {
                            // TODO:
                            return null;
                        }
                    }
                }
                else if (type == typeof(string))
                {
                    return this.convertToInt64;
                }
                else
                {
                    return null;
                }
                #endregion
            }
            else if (dbType == System.Data.DbType.String)
            {
                #region String
                if (type.IsEnum)
                {
                    return new EnumConverter(type).ConvertToDbType;
                }
                else if (type.IsGenericType)
                {
                    if (type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        Type argumentsType = type.GetGenericArguments()[0];
                        if (argumentsType.IsEnum)
                        {
                            return new EnumConverter(argumentsType).ConvertToDbType;
                        }
                        else
                        {
                            // TODO: 
                            return null;
                        }
                    }
                }
                #endregion
            }
            else if (dbType == System.Data.DbType.Guid)
            {
                #region Guid
                if (type == typeof(string) || type == typeof(byte[]))
                {
                    return this.convertToGuid;
                }
                else
                {
                    return null;
                }
                #endregion
            }
            else if ((dbType == System.Data.DbType.DateTime) || (dbType == System.Data.DbType.Date) || (dbType == System.Data.DbType.Time) || (dbType == System.Data.DbType.DateTime2))
            {
                #region DateTime
                if (type == typeof(string))
                {
                    return this.convertToDateTime;
                }
                else
                {
                    return null;
                }
                #endregion
            }
            else
            {
                // TODO:
                return null;
            }
            return null;
        }
    }
}
