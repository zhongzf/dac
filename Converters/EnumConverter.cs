using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RaisingStudio.Data.Converters
{
    public class EnumConverter
    {
        private System.Type _type;
        private System.ComponentModel.EnumConverter _enumConverter;

        public EnumConverter(System.Type type)
        {
            this._type = type;
            this._enumConverter = new System.ComponentModel.EnumConverter(this._type);
        }

        public object ConvertFromDbType(object value)
        {
            if ((value == null) || Convert.IsDBNull(value))
            {
                return null;
            }
            return this._enumConverter.ConvertFromString(value as string);
        }

        public object ConvertToDbType(object value)
        {
            if (value == null)
            {
                return value;
            }
            return this._enumConverter.ConvertToString(value);
        }
    }
}
