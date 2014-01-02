using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Reflection;
using System.Text;
using RaisingStudio.Data.Common;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Text.RegularExpressions;
using System.Data;
using System.Linq.Expressions;
using System.Collections;

namespace RaisingStudio.Data.Settings
{
    [ConfigurationCollection(typeof(CommandElement), AddItemName = "Command", CollectionType = ConfigurationElementCollectionType.BasicMap)]
    public sealed class Commands : ConfigurationSection
    {
        public const string CONFIGURATION_COMMANDS_SECTION_NAME = "raisingstudio.data/Commands";

        private static Commands GetConfig()
        {
            string sectionName = CONFIGURATION_COMMANDS_SECTION_NAME;
            Commands commands = (Commands)ConfigurationManager.GetSection(sectionName);
            commands.commandElements = new Dictionary<string, CommandElement>();
            foreach (CommandElement command in commands.CommandCollection)
            {
                commands.commandElements.Add(command.Name, command);
            }
            return commands;
        }

        #region Default Instance
        private static volatile Commands _default;
        private static object default_lock = new object();
        public static Commands Default
        {
            get
            {
                if (_default == null)
                {
                    lock (default_lock)
                    {
                        if (_default == null)
                        {
                            _default = GetConfig();
                        }
                    }
                }
                return _default;
            }
        }
        #endregion

        public Commands()
        {
        }

        private Dictionary<string, CommandElement> commandElements;
        public Dictionary<string, CommandElement> CommandElements
        {
            get
            {
                return this.commandElements;
            }
        }

        [ConfigurationProperty("", IsDefaultCollection = true)]
        public CommandCollection CommandCollection
        {
            get
            {
                return (CommandCollection)base[""];
            }
        }

        [ConfigurationProperty("DefaultSettings", IsRequired = false)]
        public CommandDefaultSettingsElement DefaultSettings
        {
            get { return (CommandDefaultSettingsElement)this["DefaultSettings"]; }
            set { this["CommandText"] = value; }
        }

        #region GetCommand
        public static Command GetCommand(string name)
        {
            CommandElement commandElement = Default.CommandElements[name];
            if (commandElement != null)
            {
                Command command = new Command
                {
                    CommandText = commandElement.CommandText.Value != null ? commandElement.CommandText.Value.Trim() : null,
                    CommandType = commandElement.CommandType,
                    CommandTimeout = commandElement.CommandTimeout,
                    ParameterPrefix = commandElement.ParameterPrefix,
                    UseParameterPrefixInSql = commandElement.UseParameterPrefixInSql,
                    UseParameterPrefixInParameter = commandElement.UseParameterPrefixInParameter,
                    UseBrackets = commandElement.UseBrackets
                };
                foreach (ParameterElement parameterElement in commandElement.Parameters)
                {
                    Parameter parameter = new Parameter
                    {
                        ParameterName = parameterElement.ParameterName,
                        DbType = parameterElement.DbType,
                        Value = parameterElement.Value,
                        IsNullable = parameterElement.IsNullable,
                        SourceVersion = parameterElement.SourceVersion,
                        SourceColumn = parameterElement.SourceColumn,
                        SourceColumnNullMapping = parameterElement.SourceColumnNullMapping,
                        Precision = parameterElement.Precision,
                        Scale = parameterElement.Scale,
                        Size = parameterElement.Size
                    };
                    command.Parameters.Add(parameter);
                }
                return command;
            }
            return null;
        }

        public static Command GetCommand(string name, params object[] parameters)
        {
            Command command = GetCommand(name);
            if (command != null)
            {
                for (int i = 0; i < parameters.Length; i++)
                {
                    command.Parameters[i].Value = parameters[i];
                }
            }
            return command;
        }

        public static Command GetCommand(string name, object parameters)
        {
            Command command = GetCommand(name);
            if (command != null)
            {
                if ((parameters == null) || (parameters is DBNull))
                {
                    command.Parameters[0].Value = parameters;
                }
                else
                {
                    Type type = parameters.GetType();
                    if (TypeManager.IsWellKnownDataType(type))
                    {
                        command.Parameters[0].Value = parameters;
                    }
                    else
                    {
                        if (parameters is IDictionary)
                        {
                            for (int i = 0; i < command.Parameters.Count; i++)
                            {
                                Parameter parameter = command.Parameters[i];
                                string parameterName = parameter.ParameterName;
                                string pureParameterName = command.UseParameterPrefixInParameter ? (parameterName.StartsWith(command.ParameterPrefix) ? parameterName.Substring(command.ParameterPrefix.Length, parameterName.Length - command.ParameterPrefix.Length) : parameterName) : parameterName;
                                foreach (object key in ((IDictionary)parameters).Keys)
                                {
                                    if (Convert.ToString(key) == pureParameterName)
                                    {
                                        object value = ((IDictionary)parameters)[pureParameterName];
                                        command.Parameters[i].Value = value;
                                        break;
                                    }
                                }
                            }
                        }
                        else if (parameters is ICollection)
                        {
                            int i = 0;
                            foreach (object value in (ICollection)parameters)
                            {
                                command.Parameters[i].Value = value;
                            }
                        }
                        else
                        {
                            for (int i = 0; i < command.Parameters.Count; i++)
                            {
                                Parameter parameter = command.Parameters[i];
                                string parameterName = parameter.ParameterName;
                                string pureParameterName = command.UseParameterPrefixInParameter ? (parameterName.StartsWith(command.ParameterPrefix) ? parameterName.Substring(command.ParameterPrefix.Length, parameterName.Length - command.ParameterPrefix.Length) : parameterName) : parameterName;
                                PropertyInfo property = type.GetProperty(pureParameterName);
                                if (property != null)
                                {
                                    parameter.Value = property.GetValue(parameters, null);
                                }
                            }
                        }
                    }
                }
            }
            return command;
        }
        #endregion
    }

    public class CommandDefaultSettingsElement : ConfigurationElement
    {
        #region Default Instance
        private static volatile CommandDefaultSettingsElement _default;
        public static CommandDefaultSettingsElement Default
        {
            get
            {
                return _default;
            }
        }
        #endregion
        
        public CommandDefaultSettingsElement()
        {
            this.ParameterPrefix = "@";
            this.UseParameterPrefixInSql = true;
            this.UseParameterPrefixInParameter = true;
            this.UseBrackets = true;

            _default = this;
        }

        [ConfigurationProperty("parameterPrefix", IsRequired = false)]
        [DefaultValue("@")]
        public string ParameterPrefix
        {
            get { return (string)this["parameterPrefix"]; }
            set { this["parameterPrefix"] = value; }
        }

        [ConfigurationProperty("useParameterPrefixInSql", IsRequired = false)]
        [DefaultValue(true)]
        public bool UseParameterPrefixInSql
        {
            get { return (bool)this["useParameterPrefixInSql"]; }
            set { this["useParameterPrefixInSql"] = value; }
        }

        [ConfigurationProperty("useParameterPrefixInParameter", IsRequired = false)]
        [DefaultValue(true)]
        public bool UseParameterPrefixInParameter
        {
            get { return (bool)this["useParameterPrefixInParameter"]; }
            set { this["useParameterPrefixInParameter"] = value; }
        }

        [ConfigurationProperty("useBrackets", IsRequired = false)]
        [DefaultValue(true)]
        public bool UseBrackets
        {
            get { return (bool)this["useBrackets"]; }
            set { this["useBrackets"] = value; }
        }
    }

    [ConfigurationCollection(typeof(CommandElement), AddItemName = "Command", CollectionType = ConfigurationElementCollectionType.BasicMap)]
    public class CommandCollection : ConfigurationElementCollection
    {
        protected override ConfigurationElement CreateNewElement()
        {
            return new CommandElement();
        }

        protected override object GetElementKey(ConfigurationElement element)
        {
            return ((CommandElement)element).Name;
        }
    }

    public class CommandElement : ConfigurationElement
    {
        [ConfigurationProperty("name", IsRequired = true)]
        public string Name
        {
            get { return (string)this["name"]; }
            set { this["name"] = value; }
        }

        [ConfigurationProperty("CommandText", IsRequired = false)]
        public CommandTextElement CommandText
        {
            get { return (CommandTextElement)this["CommandText"]; }
            set { this["CommandText"] = value; }
        }

        [ConfigurationProperty("Parameters", IsRequired = false)]
        public ParameterCollectionElement Parameters
        {
            get { return (ParameterCollectionElement)this["Parameters"]; }
            set { this["Parameters"] = value; }
        }


        public CommandElement()
        {
            this.CommandType = System.Data.CommandType.Text;
            this.CommandTimeout = 30;

            this.ParameterPrefix = CommandDefaultSettingsElement.Default.ParameterPrefix;// "@";
            this.UseParameterPrefixInSql = CommandDefaultSettingsElement.Default.UseParameterPrefixInSql;// true;
            this.UseParameterPrefixInParameter = CommandDefaultSettingsElement.Default.UseParameterPrefixInParameter;// true;
            this.UseBrackets = CommandDefaultSettingsElement.Default.UseBrackets;// true;
        }

        [DefaultValue(System.Data.CommandType.Text)]
        [ConfigurationProperty("commandType", IsRequired = false)]
        public System.Data.CommandType CommandType
        {
            get { return (System.Data.CommandType)this["commandType"]; }
            set { this["commandType"] = value; }
        }

        [DefaultValue(30)]
        [ConfigurationProperty("commandTimeout", IsRequired = false)]
        public int CommandTimeout
        {
            get { return (int)this["commandTimeout"]; }
            set { this["commandTimeout"] = value; }
        }


        [ConfigurationProperty("parameterPrefix", IsRequired = false)]
        [DefaultValue("@")]
        public string ParameterPrefix
        {
            get { return (string)this["parameterPrefix"]; }
            set { this["parameterPrefix"] = value; }
        }

        [ConfigurationProperty("useParameterPrefixInSql", IsRequired = false)]
        [DefaultValue(true)]
        public bool UseParameterPrefixInSql
        {
            get { return (bool)this["useParameterPrefixInSql"]; }
            set { this["useParameterPrefixInSql"] = value; }
        }

        [ConfigurationProperty("useParameterPrefixInParameter", IsRequired = false)]
        [DefaultValue(true)]
        public bool UseParameterPrefixInParameter
        {
            get { return (bool)this["useParameterPrefixInParameter"]; }
            set { this["useParameterPrefixInParameter"] = value; }
        }

        [ConfigurationProperty("useBrackets", IsRequired = false)]
        [DefaultValue(true)]
        public bool UseBrackets
        {
            get { return (bool)this["useBrackets"]; }
            set { this["useBrackets"] = value; }
        }
    }

    public class CommandTextElement : ConfigurationElement
    {
        protected override void DeserializeElement(System.Xml.XmlReader reader, bool serializeCollectionKey)
        {
            Value = reader.ReadElementContentAsString();
        }

        protected override bool SerializeElement(System.Xml.XmlWriter writer, bool serializeCollectionKey)
        {
            if (writer != null)
            {
                writer.WriteCData(Value);
            }
            return true;
        }

        [ConfigurationProperty("content", IsRequired = false)]
        public string Value
        {
            get { return (string)this["content"]; }
            set { this["content"] = value; }
        }
    }

    [ConfigurationCollection(typeof(ParameterElement), AddItemName = "Parameter", CollectionType = ConfigurationElementCollectionType.BasicMap)]
    public class ParameterCollectionElement : ConfigurationElementCollection
    {
        protected override ConfigurationElement CreateNewElement()
        {
            return new ParameterElement();
        }

        protected override object GetElementKey(ConfigurationElement element)
        {
            return ((ParameterElement)element).ParameterName;
        }
    }

    public class ParameterElement : ConfigurationElement
    {
        [ConfigurationProperty("parameterName", IsRequired = true)]
        public string ParameterName
        {
            get { return (string)this["parameterName"]; }
            set { this["parameterName"] = value; }
        }

        [ConfigurationProperty("dbType", IsRequired = false)]
        public System.Data.DbType DbType
        {
            get { return (System.Data.DbType)this["dbType"]; }
            set { this["dbType"] = value; }
        }

        protected override void DeserializeElement(System.Xml.XmlReader reader, bool serializeCollectionKey)
        {
            string outerXml = reader.ReadOuterXml();
            var contentXmlReader = System.Xml.XmlReader.Create(new StringReader(outerXml));
            if (contentXmlReader.Read())
            {
                object content = contentXmlReader.ReadElementContentAsObject();
                if ((content != null) && ((content is string) ? !string.IsNullOrEmpty((string)content) : true))
                {
                    Value = content;
                    string xmlDocument = Regex.Replace(outerXml, "(^<Parameter.*?>)(.*?)(</Parameter>)$", "$1$3");
                    var elementXmlReader = System.Xml.XmlReader.Create(new StringReader(xmlDocument));
                    if (elementXmlReader.Read())
                    {
                        base.DeserializeElement(elementXmlReader, serializeCollectionKey);
                    }
                }
                else
                {
                    string xmlDocument = outerXml;
                    var elementXmlReader = System.Xml.XmlReader.Create(new StringReader(xmlDocument));
                    if (elementXmlReader.Read())
                    {
                        base.DeserializeElement(elementXmlReader, serializeCollectionKey);
                    }
                }
            }
        }

        private object value;
        public object Value
        {
            get { return this.value; }
            set { this.value = value; }
        }

        public ParameterElement()
        {
            this.Direction = ParameterDirection.Input;
            this.IsNullable = true;
            this.SourceVersion = DataRowVersion.Default;
        }

        [ConfigurationProperty("direction", IsRequired = false)]
        [DefaultValue(ParameterDirection.Input)]
        public System.Data.ParameterDirection Direction
        {
            get { return (System.Data.ParameterDirection)this["direction"]; }
            set { this["direction"] = value; }
        }

        [ConfigurationProperty("nullable", IsRequired = false)]
        public bool IsNullable
        {
            get { return (bool)this["nullable"]; }
            set { this["nullable"] = value; }
        }

        [ConfigurationProperty("sourceVersion", IsRequired = false)]
        [DefaultValue(DataRowVersion.Default)]
        public System.Data.DataRowVersion SourceVersion
        {
            get { return (System.Data.DataRowVersion)this["sourceVersion"]; }
            set { this["sourceVersion"] = value; }
        }

        [ConfigurationProperty("sourceColumn", IsRequired = false)]
        public string SourceColumn
        {
            get { return (string)this["sourceColumn"]; }
            set { this["sourceColumn"] = value; }
        }

        [ConfigurationProperty("sourceColumnNullMapping", IsRequired = false)]
        public bool SourceColumnNullMapping
        {
            get { return (bool)this["sourceColumnNullMapping"]; }
            set { this["sourceColumnNullMapping"] = value; }
        }

        [ConfigurationProperty("precision", IsRequired = false)]
        public byte Precision
        {
            get { return (byte)this["precision"]; }
            set { this["precision"] = value; }
        }

        [ConfigurationProperty("scale", IsRequired = false)]
        public byte Scale
        {
            get { return (byte)this["scale"]; }
            set { this["scale"] = value; }
        }

        [ConfigurationProperty("size", IsRequired = false)]
        public int Size
        {
            get { return (int)this["size"]; }
            set { this["size"] = value; }
        }
    }
}
