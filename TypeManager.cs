using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Text.RegularExpressions;

namespace RaisingStudio.Data
{
    public class TypeManager
    {
        public static Dictionary<string, System.Type> WellKnownDataTypes;
        public static Dictionary<System.Type, string> WellKnownDataTypeNames;
        public static Dictionary<string, System.Data.DbType> WellKnownDbTypes;
        public static Dictionary<System.Data.DbType, string> WellKnownDbTypeNames;

        static TypeManager()
        {
            #region WellKnownDataTypes
            WellKnownDataTypes = new Dictionary<string, System.Type>();
            WellKnownDataTypeNames = new Dictionary<System.Type, string>();
            WellKnownDataTypeNames.Add(typeof(short), "short");
            WellKnownDataTypes.Add("short", typeof(short));
            WellKnownDataTypes.Add("Short", typeof(short));
            WellKnownDataTypes.Add("SHORT", typeof(short));
            WellKnownDataTypes.Add("int16", typeof(short));
            WellKnownDataTypes.Add("Int16", typeof(short));
            WellKnownDataTypes.Add("INT16", typeof(short));

            WellKnownDataTypeNames.Add(typeof(int), "int");
            WellKnownDataTypes.Add("int", typeof(int));
            WellKnownDataTypes.Add("Int", typeof(int));
            WellKnownDataTypes.Add("INT", typeof(int));
            WellKnownDataTypes.Add("int32", typeof(int));
            WellKnownDataTypes.Add("Int32", typeof(int));
            WellKnownDataTypes.Add("INT32", typeof(int));

            WellKnownDataTypeNames.Add(typeof(long), "long");
            WellKnownDataTypes.Add("long", typeof(long));
            WellKnownDataTypes.Add("Long", typeof(long));
            WellKnownDataTypes.Add("LONG", typeof(long));
            WellKnownDataTypes.Add("int64", typeof(long));
            WellKnownDataTypes.Add("Int64", typeof(long));
            WellKnownDataTypes.Add("INT64", typeof(long));
            
            WellKnownDataTypeNames.Add(typeof(ushort), "ushort");
            WellKnownDataTypes.Add("ushort", typeof(ushort));
            WellKnownDataTypes.Add("UShort", typeof(ushort));
            WellKnownDataTypes.Add("USHORT", typeof(ushort));
            WellKnownDataTypes.Add("uint16", typeof(ushort));
            WellKnownDataTypes.Add("UInt16", typeof(ushort));
            WellKnownDataTypes.Add("UINT16", typeof(ushort));

            WellKnownDataTypeNames.Add(typeof(uint), "uint");
            WellKnownDataTypes.Add("uint", typeof(uint));
            WellKnownDataTypes.Add("UInt", typeof(uint));
            WellKnownDataTypes.Add("UINT", typeof(uint));
            WellKnownDataTypes.Add("uint32", typeof(uint));
            WellKnownDataTypes.Add("UInt32", typeof(uint));
            WellKnownDataTypes.Add("UINT32", typeof(uint));

            WellKnownDataTypeNames.Add(typeof(ulong), "ulong");
            WellKnownDataTypes.Add("ulong", typeof(ulong));
            WellKnownDataTypes.Add("ULong", typeof(ulong));
            WellKnownDataTypes.Add("ULONG", typeof(ulong));
            WellKnownDataTypes.Add("uint64", typeof(ulong));
            WellKnownDataTypes.Add("UInt64", typeof(ulong));
            WellKnownDataTypes.Add("UINT64", typeof(ulong));
            
            WellKnownDataTypeNames.Add(typeof(byte), "byte");
            WellKnownDataTypes.Add("byte", typeof(byte));
            WellKnownDataTypes.Add("Byte", typeof(byte));
            WellKnownDataTypes.Add("BYTE", typeof(byte));

            WellKnownDataTypeNames.Add(typeof(byte[]), "byte[]");
            WellKnownDataTypes.Add("byte[]", typeof(byte[]));
            WellKnownDataTypes.Add("Byte[]", typeof(byte[]));
            WellKnownDataTypes.Add("BYTE[]", typeof(byte[]));
            WellKnownDataTypes.Add("image", typeof(byte[]));
            WellKnownDataTypes.Add("Image", typeof(byte[]));
            WellKnownDataTypes.Add("IMAGE", typeof(byte[]));
            WellKnownDataTypes.Add("binary", typeof(byte[]));
            WellKnownDataTypes.Add("Binary", typeof(byte[]));
            WellKnownDataTypes.Add("BINARY", typeof(byte[]));

            WellKnownDataTypeNames.Add(typeof(bool), "bool");
            WellKnownDataTypes.Add("bool", typeof(bool));
            WellKnownDataTypes.Add("Bool", typeof(bool));
            WellKnownDataTypes.Add("BOOL", typeof(bool));
            WellKnownDataTypes.Add("boolean", typeof(bool));
            WellKnownDataTypes.Add("Boolean", typeof(bool));
            WellKnownDataTypes.Add("BOOLEAN", typeof(bool));

            WellKnownDataTypeNames.Add(typeof(float), "float");
            WellKnownDataTypes.Add("float", typeof(float));
            WellKnownDataTypes.Add("Float", typeof(float));
            WellKnownDataTypes.Add("FLOAT", typeof(float));
            WellKnownDataTypes.Add("single", typeof(float));
            WellKnownDataTypes.Add("Single", typeof(float));
            WellKnownDataTypes.Add("SINGLE", typeof(float));

            WellKnownDataTypeNames.Add(typeof(double), "double");
            WellKnownDataTypes.Add("double", typeof(double));
            WellKnownDataTypes.Add("Double", typeof(double));
            WellKnownDataTypes.Add("DOUBLE", typeof(double));

            WellKnownDataTypeNames.Add(typeof(decimal), "decimal");
            WellKnownDataTypes.Add("decimal", typeof(decimal));
            WellKnownDataTypes.Add("Decimal", typeof(decimal));
            WellKnownDataTypes.Add("DECIMAL", typeof(decimal));

            WellKnownDataTypeNames.Add(typeof(DateTime), "DateTime");
            WellKnownDataTypes.Add("datetime", typeof(DateTime));
            WellKnownDataTypes.Add("dateTime", typeof(DateTime));
            WellKnownDataTypes.Add("DateTime", typeof(DateTime));
            WellKnownDataTypes.Add("DATETIME", typeof(DateTime));

            WellKnownDataTypeNames.Add(typeof(Guid), "Guid");
            WellKnownDataTypes.Add("guid", typeof(Guid));
            WellKnownDataTypes.Add("Guid", typeof(Guid));
            WellKnownDataTypes.Add("GUID", typeof(Guid));

            WellKnownDataTypeNames.Add(typeof(string), "string");
            WellKnownDataTypes.Add("string", typeof(string));
            WellKnownDataTypes.Add("String", typeof(string));
            WellKnownDataTypes.Add("STRING", typeof(string));

            WellKnownDataTypeNames.Add(typeof(object), "object");
            WellKnownDataTypes.Add("object", typeof(object));
            WellKnownDataTypes.Add("Object", typeof(object));
            WellKnownDataTypes.Add("OBJECT", typeof(object));
            #endregion
            #region WellKnownDbTypes
            WellKnownDbTypes = new Dictionary<string, System.Data.DbType>();
            WellKnownDbTypeNames = new Dictionary<System.Data.DbType, string>();
            WellKnownDbTypes.Add("short", System.Data.DbType.Int16);
            WellKnownDbTypes.Add("Short", System.Data.DbType.Int16);
            WellKnownDbTypes.Add("SHORT", System.Data.DbType.Int16);
            WellKnownDbTypes.Add("int16", System.Data.DbType.Int16);
            WellKnownDbTypes.Add("Int16", System.Data.DbType.Int16);
            WellKnownDbTypes.Add("INT16", System.Data.DbType.Int16);
            WellKnownDbTypes.Add("tinyint", System.Data.DbType.Int16);
            WellKnownDbTypes.Add("Tinyint", System.Data.DbType.Int16);
            WellKnownDbTypes.Add("TinyInt", System.Data.DbType.Int16);
            WellKnownDbTypes.Add("TinyINT", System.Data.DbType.Int16);
            WellKnownDbTypes.Add("TINYINT", System.Data.DbType.Int16);
            WellKnownDbTypeNames.Add(System.Data.DbType.Int16, "short");

            WellKnownDbTypes.Add("ushort", System.Data.DbType.UInt16);
            WellKnownDbTypes.Add("Ushort", System.Data.DbType.UInt16);
            WellKnownDbTypes.Add("UShort", System.Data.DbType.UInt16);
            WellKnownDbTypes.Add("USHORT", System.Data.DbType.UInt16);
            WellKnownDbTypeNames.Add(System.Data.DbType.UInt16, "ushort");
            
            WellKnownDbTypes.Add("int", System.Data.DbType.Int32);
            WellKnownDbTypes.Add("Int", System.Data.DbType.Int32);
            WellKnownDbTypes.Add("INT", System.Data.DbType.Int32);
            WellKnownDbTypes.Add("int32", System.Data.DbType.Int32);
            WellKnownDbTypes.Add("Int32", System.Data.DbType.Int32);
            WellKnownDbTypes.Add("INT32", System.Data.DbType.Int32);
            WellKnownDbTypeNames.Add(System.Data.DbType.Int32, "int");

            WellKnownDbTypes.Add("uint", System.Data.DbType.UInt32);
            WellKnownDbTypes.Add("Uint", System.Data.DbType.UInt32);
            WellKnownDbTypes.Add("UInt", System.Data.DbType.UInt32);
            WellKnownDbTypes.Add("UINT", System.Data.DbType.UInt32);
            WellKnownDbTypeNames.Add(System.Data.DbType.UInt32, "uint");

            WellKnownDbTypes.Add("long", System.Data.DbType.Int64);
            WellKnownDbTypes.Add("Long", System.Data.DbType.Int64);
            WellKnownDbTypes.Add("LONG", System.Data.DbType.Int64);
            WellKnownDbTypes.Add("int64", System.Data.DbType.Int64);
            WellKnownDbTypes.Add("Int64", System.Data.DbType.Int64);
            WellKnownDbTypes.Add("INT64", System.Data.DbType.Int64);
            WellKnownDbTypes.Add("bigint", System.Data.DbType.Int64);
            WellKnownDbTypes.Add("Bigint", System.Data.DbType.Int64);
            WellKnownDbTypes.Add("BigInt", System.Data.DbType.Int64);
            WellKnownDbTypes.Add("BigINT", System.Data.DbType.Int64);
            WellKnownDbTypes.Add("BIGINT", System.Data.DbType.Int64);
            WellKnownDbTypes.Add("integer", System.Data.DbType.Int64);
            WellKnownDbTypes.Add("Integer", System.Data.DbType.Int64);
            WellKnownDbTypes.Add("INTEGER", System.Data.DbType.Int64);
            WellKnownDbTypeNames.Add(System.Data.DbType.Int64, "long");
            
            WellKnownDbTypes.Add("ulong", System.Data.DbType.UInt64);
            WellKnownDbTypes.Add("Ulong", System.Data.DbType.UInt64);
            WellKnownDbTypes.Add("ULong", System.Data.DbType.UInt64);
            WellKnownDbTypes.Add("ULONG", System.Data.DbType.UInt64);
            WellKnownDbTypeNames.Add(System.Data.DbType.UInt64, "ulong");

            WellKnownDbTypes.Add("byte", System.Data.DbType.Byte);
            WellKnownDbTypes.Add("Byte", System.Data.DbType.Byte);
            WellKnownDbTypes.Add("BYTE", System.Data.DbType.Byte);
            WellKnownDbTypeNames.Add(System.Data.DbType.Byte, "byte");

            WellKnownDbTypes.Add("byte[]", System.Data.DbType.Binary);
            WellKnownDbTypes.Add("Byte[]", System.Data.DbType.Binary);
            WellKnownDbTypes.Add("BYTE[]", System.Data.DbType.Binary);
            WellKnownDbTypes.Add("image", System.Data.DbType.Binary);
            WellKnownDbTypes.Add("Image", System.Data.DbType.Binary);
            WellKnownDbTypes.Add("IMAGE", System.Data.DbType.Binary);
            WellKnownDbTypes.Add("binary", System.Data.DbType.Binary);
            WellKnownDbTypes.Add("Binary", System.Data.DbType.Binary);
            WellKnownDbTypes.Add("BINARY", System.Data.DbType.Binary);
            WellKnownDbTypeNames.Add(System.Data.DbType.Binary, "byte[]");

            WellKnownDbTypes.Add("bool", System.Data.DbType.Boolean);
            WellKnownDbTypes.Add("Bool", System.Data.DbType.Boolean);
            WellKnownDbTypes.Add("BOOL", System.Data.DbType.Boolean);
            WellKnownDbTypes.Add("boolean", System.Data.DbType.Boolean);
            WellKnownDbTypes.Add("Boolean", System.Data.DbType.Boolean);
            WellKnownDbTypes.Add("BOOLEAN", System.Data.DbType.Boolean);
            WellKnownDbTypes.Add("bit", System.Data.DbType.Boolean);
            WellKnownDbTypes.Add("Bit", System.Data.DbType.Boolean);
            WellKnownDbTypes.Add("BIT", System.Data.DbType.Boolean);
            WellKnownDbTypeNames.Add(System.Data.DbType.Boolean, "bool");

            WellKnownDbTypes.Add("float", System.Data.DbType.Single);
            WellKnownDbTypes.Add("Float", System.Data.DbType.Single);
            WellKnownDbTypes.Add("FLOAT", System.Data.DbType.Single);
            WellKnownDbTypes.Add("single", System.Data.DbType.Single);
            WellKnownDbTypes.Add("Single", System.Data.DbType.Single);
            WellKnownDbTypes.Add("SINGLE", System.Data.DbType.Single);
            WellKnownDbTypeNames.Add(System.Data.DbType.Single, "float");

            WellKnownDbTypes.Add("double", System.Data.DbType.Double);
            WellKnownDbTypes.Add("Double", System.Data.DbType.Double);
            WellKnownDbTypes.Add("DOUBLE", System.Data.DbType.Double);
            WellKnownDbTypeNames.Add(System.Data.DbType.Double, "double");

            WellKnownDbTypes.Add("decimal", System.Data.DbType.Decimal);
            WellKnownDbTypes.Add("Decimal", System.Data.DbType.Decimal);
            WellKnownDbTypes.Add("DECIMAL", System.Data.DbType.Decimal);
            WellKnownDbTypes.Add("money", System.Data.DbType.Decimal);
            WellKnownDbTypes.Add("Money", System.Data.DbType.Decimal);
            WellKnownDbTypes.Add("MONEY", System.Data.DbType.Decimal);
            WellKnownDbTypes.Add("smallmoney", System.Data.DbType.Decimal);
            WellKnownDbTypes.Add("Smallmoney", System.Data.DbType.Decimal);
            WellKnownDbTypes.Add("SmallMoney", System.Data.DbType.Decimal);
            WellKnownDbTypes.Add("SMALLMONEY", System.Data.DbType.Decimal);
            WellKnownDbTypes.Add("numeric", System.Data.DbType.Decimal);
            WellKnownDbTypes.Add("Numeric", System.Data.DbType.Decimal);
            WellKnownDbTypes.Add("NUMERIC", System.Data.DbType.Decimal);
            WellKnownDbTypes.Add("number", System.Data.DbType.Decimal);
            WellKnownDbTypes.Add("Number", System.Data.DbType.Decimal);
            WellKnownDbTypes.Add("NUMBER", System.Data.DbType.Decimal);
            WellKnownDbTypeNames.Add(System.Data.DbType.Decimal, "decimal");

            WellKnownDbTypes.Add("datetime", System.Data.DbType.DateTime);
            WellKnownDbTypes.Add("Datetime", System.Data.DbType.DateTime);
            WellKnownDbTypes.Add("dateTime", System.Data.DbType.DateTime);
            WellKnownDbTypes.Add("DateTime", System.Data.DbType.DateTime);
            WellKnownDbTypes.Add("DATETIME", System.Data.DbType.DateTime);
            WellKnownDbTypes.Add("datetime2", System.Data.DbType.DateTime);
            WellKnownDbTypes.Add("Datetime2", System.Data.DbType.DateTime);
            WellKnownDbTypes.Add("dateTime2", System.Data.DbType.DateTime);
            WellKnownDbTypes.Add("DateTime2", System.Data.DbType.DateTime);
            WellKnownDbTypes.Add("DATETIME2", System.Data.DbType.DateTime);
            WellKnownDbTypes.Add("date", System.Data.DbType.DateTime);
            WellKnownDbTypes.Add("Date", System.Data.DbType.DateTime);
            WellKnownDbTypes.Add("DATE", System.Data.DbType.DateTime);
            WellKnownDbTypes.Add("time", System.Data.DbType.DateTime);
            WellKnownDbTypes.Add("Time", System.Data.DbType.DateTime);
            WellKnownDbTypes.Add("TIME", System.Data.DbType.DateTime);
            WellKnownDbTypeNames.Add(System.Data.DbType.DateTime, "DateTime");

            WellKnownDbTypes.Add("guid", System.Data.DbType.Guid);
            WellKnownDbTypes.Add("Guid", System.Data.DbType.Guid);
            WellKnownDbTypes.Add("GUID", System.Data.DbType.Guid);
            WellKnownDbTypes.Add("uniqueidentifier", System.Data.DbType.Guid);
            WellKnownDbTypes.Add("Uniqueidentifier", System.Data.DbType.Guid);
            WellKnownDbTypes.Add("UniqueIdentifier", System.Data.DbType.Guid);
            WellKnownDbTypes.Add("UNIQUEIDENTIFIER", System.Data.DbType.Guid);
            WellKnownDbTypeNames.Add(System.Data.DbType.Guid, "Guid");

            WellKnownDbTypes.Add("string", System.Data.DbType.String);
            WellKnownDbTypes.Add("String", System.Data.DbType.String);
            WellKnownDbTypes.Add("STRING", System.Data.DbType.String);
            WellKnownDbTypes.Add("char", System.Data.DbType.String);
            WellKnownDbTypes.Add("CHAR", System.Data.DbType.String);
            WellKnownDbTypes.Add("nchar", System.Data.DbType.String);
            WellKnownDbTypes.Add("Nchar", System.Data.DbType.String);
            WellKnownDbTypes.Add("NChar", System.Data.DbType.String);
            WellKnownDbTypes.Add("NCHAR", System.Data.DbType.String);
            WellKnownDbTypes.Add("varchar", System.Data.DbType.String);
            WellKnownDbTypes.Add("Varchar", System.Data.DbType.String);
            WellKnownDbTypes.Add("VarChar", System.Data.DbType.String);
            WellKnownDbTypes.Add("VARCHAR", System.Data.DbType.String);
            WellKnownDbTypes.Add("nvarchar", System.Data.DbType.String);
            WellKnownDbTypes.Add("Nvarchar", System.Data.DbType.String);
            WellKnownDbTypes.Add("NVarchar", System.Data.DbType.String);
            WellKnownDbTypes.Add("NvarChar", System.Data.DbType.String);
            WellKnownDbTypes.Add("NVarChar", System.Data.DbType.String);
            WellKnownDbTypes.Add("NVARCHAR", System.Data.DbType.String);
            WellKnownDbTypes.Add("text", System.Data.DbType.String);
            WellKnownDbTypes.Add("TEXT", System.Data.DbType.String);
            WellKnownDbTypes.Add("ntext", System.Data.DbType.String);
            WellKnownDbTypes.Add("Ntext", System.Data.DbType.String);
            WellKnownDbTypes.Add("NText", System.Data.DbType.String);
            WellKnownDbTypes.Add("NTEXT", System.Data.DbType.String);
            WellKnownDbTypes.Add("tinytext", System.Data.DbType.String);
            WellKnownDbTypes.Add("Tinytext", System.Data.DbType.String);
            WellKnownDbTypes.Add("TinyText", System.Data.DbType.String);
            WellKnownDbTypes.Add("TINYTEXT", System.Data.DbType.String);
            WellKnownDbTypes.Add("mediumtext", System.Data.DbType.String);
            WellKnownDbTypes.Add("Mediumtext", System.Data.DbType.String);
            WellKnownDbTypes.Add("MediumText", System.Data.DbType.String);
            WellKnownDbTypes.Add("MEDIUMTEXT", System.Data.DbType.String);
            WellKnownDbTypes.Add("longtext", System.Data.DbType.String);
            WellKnownDbTypes.Add("Longtext", System.Data.DbType.String);
            WellKnownDbTypes.Add("LongText", System.Data.DbType.String);
            WellKnownDbTypes.Add("LONGTEXT", System.Data.DbType.String);
            WellKnownDbTypeNames.Add(System.Data.DbType.String, "string");

            WellKnownDbTypes.Add("object", System.Data.DbType.Object);
            WellKnownDbTypes.Add("Object", System.Data.DbType.Object);
            WellKnownDbTypes.Add("OBJECT", System.Data.DbType.Object);
            WellKnownDbTypeNames.Add(System.Data.DbType.Object, "object");
            #endregion
        }

        public static bool IsWellKnownDataType(System.Type type)
        {
            if ((type == typeof(string))
                || (type == typeof(int))
                || (type == typeof(DateTime))
                || (type == typeof(decimal))
                || (type == typeof(bool))
                || (type == typeof(long)) || (type == typeof(short))
                || (type == typeof(float)) || (type == typeof(double))
                || (type == typeof(byte)) || (type == typeof(byte[]))
                || (type == typeof(Guid)))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        private static Regex dbTypeNameRegex = new Regex(@"^\w{3,}");

        public static string GetDbTypeName(string dbType)
        {
            if (!string.IsNullOrWhiteSpace(dbType))
            {
                var regex = dbTypeNameRegex;
                var dbTypeName = regex.Match(dbType).Value;
                return dbTypeName.Trim();
            }
            return dbType;
        }

        public static System.Data.DbType GetWellKnownDbType(string dbType)
        {
            string dbTypeName = GetDbTypeName(dbType);
            if (!string.IsNullOrWhiteSpace(dbTypeName))
            {
                if (WellKnownDbTypes.ContainsKey(dbTypeName))
                {
                    return WellKnownDbTypes[dbTypeName];
                }
                else
                {
                    return System.Data.DbType.String;
                }
            }
            else
            {
                return System.Data.DbType.String;
            }
        }

        public static string GetWellKnownDbTypeName(System.Data.DbType dbType)
        {
            if (WellKnownDbTypeNames.ContainsKey(dbType))
            {
                return WellKnownDbTypeNames[dbType];
            }
            return dbType.ToString();
        }

        public static Type CreateDynamicType(string assemblyName, string typeName, Type baseType, string[] propertyNames, Type[] propertyTypes)
        {
#if NET_4_0
            var access = AssemblyBuilderAccess.RunAndCollect;
#else
			var access = AssemblyBuilderAccess.Run;
#endif
            AssemblyBuilder assemblyBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(new AssemblyName(assemblyName), access);
            ModuleBuilder moduleBuilder = assemblyBuilder.DefineDynamicModule(assemblyBuilder.GetName().Name);
            TypeBuilder typeBuilder = moduleBuilder.DefineType(typeName, TypeAttributes.Public | TypeAttributes.Class, baseType);
            for (int i = 0; i < propertyNames.Length; i++)
            {
                string propertyName = propertyNames[i];
                Type propertyType = propertyTypes[i];

                FieldBuilder fieldBuilder = typeBuilder.DefineField("_" + propertyName, propertyType, FieldAttributes.Private);
                PropertyBuilder propertyBuilder = typeBuilder.DefineProperty(propertyName, PropertyAttributes.None, propertyType, new Type[] { propertyType });

                // getter
                MethodBuilder getterMethodBuilder = typeBuilder.DefineMethod("get_" + propertyName, MethodAttributes.Public | MethodAttributes.HideBySig, propertyType, null);
                ILGenerator getterILGenerator = getterMethodBuilder.GetILGenerator();
                getterILGenerator.Emit(OpCodes.Ldarg_0);
                getterILGenerator.Emit(OpCodes.Ldfld, fieldBuilder);
                getterILGenerator.Emit(OpCodes.Ret);
                // setter
                MethodBuilder setterMethodBuilder = typeBuilder.DefineMethod("set_" + propertyName, MethodAttributes.Public | MethodAttributes.HideBySig, null, new Type[] { propertyType });
                ILGenerator setterILGenerator = setterMethodBuilder.GetILGenerator();
                setterILGenerator.Emit(OpCodes.Ldarg_0);
                setterILGenerator.Emit(OpCodes.Ldarg_1);
                setterILGenerator.Emit(OpCodes.Stfld, fieldBuilder);
                setterILGenerator.Emit(OpCodes.Ret);

                propertyBuilder.SetGetMethod(getterMethodBuilder);
                propertyBuilder.SetSetMethod(setterMethodBuilder);
            }
            return typeBuilder.CreateType();
        }



        public static string GetWellKnownDataTypeName(System.Type type)
        {
            if (WellKnownDataTypeNames.ContainsKey(type))
            {
                return WellKnownDataTypeNames[type];
            }
            return type.ToString();
        }

        /// <summary>
        /// Gets Wellknown Type.
        /// </summary>
        /// <param name="name">Type name.</param>
        /// <returns>Type.</returns>
        public static System.Type GetWellKnownDataType(string name)
        {
            name = name.Trim();
            if (WellKnownDataTypes.ContainsKey(name))
            {
                return WellKnownDataTypes[name];
            }
            return null;
        }
    }
}
