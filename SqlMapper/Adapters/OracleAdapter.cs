using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Extensions.SqlMapper.Adapters
{
    public class OracleAdapter : ISqlAdapter
    {
        private static string ParseSql(string input)
        {
            /*
                if (this.DataContext._Managers().DatabaseManager().GetDatabaseType() == VisualCactusDbConnectionType.Oracle)
                {
                    topQuery = "SELECT * FROM"
                        + " (SELECT Transaction_K, TransactionEchoId, TransactionType_RTK, TransactionSubscriberId, TransactionDate, CreatedDate, IsProcessed, ProcessedDate, ProcessedInfo"
                        + " FROM IMSTRANSACTION WHERE TransactionDate >= @pDate AND IsProcessed = @isProcessed AND TransactionType_RTK = @transactionType)"
                        + " WHERE ROWNUM <= @limit"
                        + " ORDER BY TransactionDate";
                }
                else
                {
                    topQuery = "SELECT TOP(@limit)"
                        + " Transaction_K, TransactionEchoId, TransactionType_RTK, TransactionSubscriberId, TransactionDate, CreatedDate, IsProcessed, ProcessedDate, ProcessedInfo"
                        + " FROM IMSTRANSACTION WHERE TransactionDate >= @pDate AND IsProcessed = @isProcessed AND TransactionType_RTK = @transactionType"
                        + " ORDER BY TransactionDate";
                }

                if (this.DataContext._Managers().DatabaseManager().GetDatabaseType() == VisualCactusDbConnectionType.Oracle)
                {
                    topQuery = "SELECT * FROM (SELECT Transaction_K FROM IMSTRANSACTION WHERE TransactionDate >= @date AND TransactionEchoId = @echoId AND TransactionType_RTK = @typeRtk) WHERE ROWNUM <= 1";
                }
                else
                {
                    topQuery = "SELECT TOP(1) Transaction_K FROM IMSTRANSACTION WHERE TransactionDate >= @date AND TransactionEchoId = @echoId AND TransactionType_RTK = @typeRtk";
                }
            */

            var r = input;

            // TODO: Oracle SQL overrides

            // SQL TOP
            var top = "select top(";
            if (r.StartsWith(top))
            {
                var topStartIndex = r.IndexOf(top) + 10;
                var topEndIndex = 1;
            }

            throw new NotImplementedException("ORACLE OVERRIDES INCOMPLETE");

            //return r;
        }

        bool ISqlAdapter.Delete(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, IEnumerable<PropertyInfo> keyProperties, object entityToDelete)
        {
            // Build base sql
            var sb = new StringBuilder();
            sb.AppendFormat("delete from {0} where ", tableName);

            // Build parameters
            for (var i = 0; i < keyProperties.Count(); i++)
            {
                var property = keyProperties.ElementAt(i);
                sb.AppendFormat("{0} = @{1}", property.Name, property.Name);
                if (i < keyProperties.Count() - 1) sb.AppendFormat(" and ");
            }

            // Execute
            var results = Dapper.SqlMapper.Execute(connection, sb.ToString(), entityToDelete, transaction: transaction, commandTimeout: commandTimeout);
            return results > 0;
        }

        T ISqlAdapter.Get<T>(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string sql, Dapper.DynamicParameters parameters)
        {
            var parsedSql = ParseSql(sql);

            // Execute
            var results = Dapper.SqlMapper.Query<T>(connection, parsedSql, parameters, transaction: transaction, commandTimeout: commandTimeout).SingleOrDefault();
            return results;
        }

        void ISqlAdapter.Insert(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, object entityToInsert)
        {
            var cmd = string.Format("insert into {0} ({1}) values ({2})", tableName, columnList, parameterList);
            Dapper.SqlMapper.Execute(connection, cmd, entityToInsert, transaction, commandTimeout);
        }

        IEnumerable<dynamic> ISqlAdapter.Query(IDbConnection connection, string sql, object param, IDbTransaction transaction, bool buffered, int? commandTimeout, CommandType? commandType)
        {
            var parsedSql = ParseSql(sql);

            var results = Dapper.SqlMapper.Query(connection, parsedSql, param, transaction, buffered, commandTimeout, commandType);
            return results;
        }

        IEnumerable<T> ISqlAdapter.Query<T>(IDbConnection connection, string sql, object param, IDbTransaction transaction, bool buffered, int? commandTimeout, CommandType? commandType)
        {
            var parsedSql = ParseSql(sql);

            var results = Dapper.SqlMapper.Query<T>(connection, parsedSql, param, transaction, buffered, commandTimeout, commandType);
            return results;
        }

        bool ISqlAdapter.Update(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string tableName, IEnumerable<PropertyInfo> keyProperties, IEnumerable<PropertyInfo> updateableProperties, object entityToUpdate)
        {
            // Build base sql
            var sb = new StringBuilder();
            sb.AppendFormat("update {0} set ", tableName);

            // Build assignments
            for (var i = 0; i < updateableProperties.Count(); i++)
            {
                var property = updateableProperties.ElementAt(i);
                sb.AppendFormat("{0} = @{1}", property.Name, property.Name);
                if (i < updateableProperties.Count() - 1) sb.AppendFormat(", ");
            }

            sb.Append(" where ");

            // Build parameters
            for (var i = 0; i < keyProperties.Count(); i++)
            {
                var property = keyProperties.ElementAt(i);
                sb.AppendFormat("{0} = @{1}", property.Name, property.Name);
                if (i < keyProperties.Count() - 1) sb.AppendFormat(" and ");
            }

            // Execute
            var results = Dapper.SqlMapper.Execute(connection, sb.ToString(), entityToUpdate, commandTimeout: commandTimeout, transaction: transaction);
            return results > 0;
        }
    }
}