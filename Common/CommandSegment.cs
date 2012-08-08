using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace RaisingStudio.Data.Common
{
    [Serializable]
    public class CommandSegment
    {
        public CommandSegment()
        {
        }

        public CommandSegment(Command command, CommandSegmentType segmentType)
        {
            this.Command = command;
            this.SegmentType = segmentType;
        }

        public Command Command { get; set; }
        public CommandSegmentType SegmentType { get; set; }
    }
}
