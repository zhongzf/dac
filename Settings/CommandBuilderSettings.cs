using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Reflection;
using System.Text;

namespace RaisingStudio.Data.Settings
{
    public class CommandBuilderSettings : ConfigurationSection
    {
        private static readonly ConfigurationProperty configurationProperty = new ConfigurationProperty(string.Empty, typeof(CommandBuilderSettingCollection), null, ConfigurationPropertyOptions.IsDefaultCollection);

        [ConfigurationProperty("", Options = ConfigurationPropertyOptions.IsDefaultCollection)]
        public CommandBuilderSettingCollection Values
        {
            get
            {
                return (CommandBuilderSettingCollection)base[configurationProperty];
            }
        }
    }


    [ConfigurationCollection(typeof(CommandBuilderSetting))]
    public class CommandBuilderSettingCollection : ConfigurationElementCollection
    {
        protected override ConfigurationElement CreateNewElement()
        {
            return new CommandBuilderSetting();
        }

        protected override object GetElementKey(ConfigurationElement element)
        {
            return ((CommandBuilderSetting)element).Name;
        }

        public CommandBuilderSetting this[int index]
        {
            get
            {
                return (CommandBuilderSetting)this.BaseGet(index);
            }
        }
    }

    public class CommandBuilderSetting : ConfigurationElement
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

        [ConfigurationProperty("useBrackets", DefaultValue = true)]
        public bool UseBrackets
        {
            get
            {
                if (this["useBrackets"] != null)
                {
                    return (bool)this["useBrackets"];
                }
                else
                {
                    return true;
                }
            }
            set
            {
                this["useBrackets"] = value;
            }
        }

        [ConfigurationProperty("pagingMethod", IsRequired = false)]
        public string PagingMethod
        {
            get
            {
                return (string)this["pagingMethod"];
            }
            set
            {
                this["pagingMethod"] = value;
            }
        }

        [ConfigurationProperty("identityMethod", IsRequired = false)]
        public string IdentityMethod
        {
            get
            {
                return (string)this["identityMethod"];
            }
            set
            {
                this["identityMethod"] = value;
            }
        }


        [ConfigurationProperty("supportsInsertSelectIdentity", IsRequired = false)]
        public bool SupportsInsertSelectIdentity
        {
            get
            {
                return (bool)this["supportsInsertSelectIdentity"];
            }
            set
            {
                this["supportsInsertSelectIdentity"] = value;
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

        public Type CommandBuilderType
        {
            get
            {
                string type = this.Type;
                return System.Type.GetType(type);
            }
        }
    }
}
