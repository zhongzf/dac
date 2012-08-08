using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RaisingStudio.Data.Converters
{
    public interface IDbTypeConverter
    {
        object ConvertFromDbType(object value);
        object ConvertToDbType(object value);
    }
}
