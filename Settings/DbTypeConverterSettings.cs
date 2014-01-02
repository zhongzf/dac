using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.Data;

namespace RaisingStudio.Data.Settings
{
    public class DbTypeConverterSettings : ConfigurationSection
    {
        private static readonly ConfigurationProperty configurationProperty = new ConfigurationProperty(string.Empty, typeof(DbTypeConverterSettingCollection), null, ConfigurationPropertyOptions.IsDefaultCollection);

        [ConfigurationProperty("", Options = ConfigurationPropertyOptions.IsDefaultCollection)]
        public DbTypeConverterSettingCollection Values
        {
            get
            {
                return (DbTypeConverterSettingCollection)base[configurationProperty];
            }
        }
    }


    [ConfigurationCollection(typeof(DbTypeConverterSetting))]
    public class DbTypeConverterSettingCollection : ConfigurationElementCollection
    {
        protected override ConfigurationElement CreateNewElement()
        {
            return new DbTypeConverterSetting();
        }

        protected override object GetElementKey(ConfigurationElement element)
        {
            return ((DbTypeConverterSetting)element).Name;
        }

        public DbTypeConverterSetting this[int index]
        {
            get
            {
                return (DbTypeConverterSetting)this.BaseGet(index);
            }
        }
    }

    public class DbTypeConverterSetting : ConfigurationElement
    {
        [ConfigurationProperty("name", IsRequired = true)]
        public string Name
        {
            get
            {
                return (string)this["name"];
            }
            set
            {
                this["name"] = value;
            }
        }

        [ConfigurationProperty("enabled", DefaultValue = true)]
        public bool Enabled
        {
            get
            {
                if (this["enabled"] != null)
                {
                    return (bool)this["enabled"];
                }
                else
                {
                    return true;
                }
            }
            set
            {
                this["enabled"] = value;
            }
        }


        [ConfigurationProperty("type", IsRequired = false)]
        public string Type
        {
            get
            {
                return (string)this["type"];
            }
            set
            {
                this["type"] = value;
            }
        }

        public Type DataType
        {
            get
            {
                string type = this.Type;
                return System.Type.GetType(type);
            }
        }


        [ConfigurationProperty("dbType", IsRequired = true)]
        public string dbType
        {
            get
            {
                return (string)this["dbType"];
            }
            set
            {
                this["dbType"] = value;
            }
        }

        public DbType DbType
        {
            get
            {
                return (DbType)Enum.Parse(typeof(DbType), dbType);
            }
        }

        [ConfigurationProperty("converter", IsRequired = false)]
        public string Converter
        {
            get
            {
                return (string)this["converter"];
            }
            set
            {
                this["converter"] = value;
            }
        }

        public Type ConverterType
        {
            get
            {
                string type = this.Converter;
                return System.Type.GetType(type);
            }
        }
    }
}
