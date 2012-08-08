using System;
using System.Data;

namespace RaisingStudio.Data.Common
{
    public interface IDatabase
    {
        int ExecuteNonQuery(Command command);
        IDataReader ExecuteReader(Command command);
        object ExecuteScalar(Command command);
    }
}
