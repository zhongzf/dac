using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RaisingStudio.Data.Converters
{
    public class IntToEnumConverter
    {
        private Type _type;

        public Type Type
        {
            get { return _type; }
            set { _type = value; }
        }

        public IntToEnumConverter(Type type)
        {
            this._type = type;
        }

        public object Convert(object value)
        {
            return System.Enum.ToObject(this._type, value);
        }
    }
}
