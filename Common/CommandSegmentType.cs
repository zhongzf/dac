using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RaisingStudio.Data.Common
{
    [Flags]
    public enum CommandSegmentType
    {
        None = 0x0000,
        Column = 0x0001,
        Parameter = 0x0002,
        Binary = 0x0004,
        Condition = 0x0008,
        Sorting = 0x0010,
        Select = 0x0020,
        Paging = 0x0040,
        Counting = 0x0080
    }
}
