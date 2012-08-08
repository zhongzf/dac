using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;

namespace RaisingStudio.Data
{
    public class CommandConverterSettings : ConfigurationSection
    {
        private static readonly ConfigurationProperty configurationProperty = new ConfigurationProperty(string.Empty, typeof(CommandConverterSettingCollection), null, ConfigurationPropertyOptions.IsDefaultCollection);

        [ConfigurationProperty("", Options = ConfigurationPropertyOptions.IsDefaultCollection)]
        public CommandConverterSettingCollection Values
        {
            get
            {
                return (CommandConverterSettingCollection)base[configurationProperty];
            }
        }
    }

    [ConfigurationCollection(typeof(CommandConverterSetting))]
    public class CommandConverterSettingCollection : ConfigurationElementCollection
    {
        protected override ConfigurationElement CreateNewElement()
        {
            return new CommandConverterSetting();
        }

        protected override object GetElementKey(ConfigurationElement element)
        {
            return ((CommandConverterSetting)element).Name;
        }

        public CommandConverterSetting this[int index]
        {
            get
            {
                return (CommandConverterSetting)this.BaseGet(index);
            }
        }
    }

    public class CommandConverterSetting : ConfigurationElement
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

        [ConfigurationProperty("providerName", IsRequired = true)]
        public string ProviderName
        {
            get
            {
                return (string)this["providerName"];
            }
            set
            {
                this["providerName"] = value;
            }
        }

        [ConfigurationProperty("usePositionalParameters", IsRequired = true)]
        public bool UsePositionalParameters
        {
            get
            {
                return (bool)this["usePositionalParameters"];
            }
            set
            {
                this["usePositionalParameters"] = value;
            }
        }

        [ConfigurationProperty("parameterPrefix", IsRequired = true)]
        public string ParameterPrefix
        {
            get
            {
                return (string)this["parameterPrefix"];
            }
            set
            {
                this["parameterPrefix"] = value;
            }
        }

        [ConfigurationProperty("useParameterPrefixInParameter", IsRequired = true)]
        public bool UseParameterPrefixInParameter
        {
            get
            {
                return (bool)this["useParameterPrefixInParameter"];
            }
            set
            {
                this["useParameterPrefixInParameter"] = value;
            }
        }

        [ConfigurationProperty("useParameterPrefixInSql", IsRequired = true)]
        public bool UseParameterPrefixInSql
        {
            get
            {
                return (bool)this["useParameterPrefixInSql"];
            }
            set
            {
                this["useParameterPrefixInParameter"] = value;
            }
        }
    }
}