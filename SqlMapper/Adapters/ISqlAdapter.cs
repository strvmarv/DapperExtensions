using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Extensions.SqlMapper.Adapters
{
    public interface ISqlAdapter
    {
        bool Delete(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, IEnumerable<PropertyInfo> keyProperties, object entityToDelete);

        T Get<T>(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string sql, Dapper.DynamicParameters parameters);

        void Insert(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, object entityToInsert);

        IEnumerable<dynamic> Query(IDbConnection connection, string sql, object param = null, IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null);

        IEnumerable<T> Query<T>(IDbConnection connection, string sql, object param = null, IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null);

        bool Update(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, IEnumerable<PropertyInfo> keyProperties, IEnumerable<PropertyInfo> updateableProperties, object entityToUpdate);
    }
}