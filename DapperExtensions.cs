using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Timers;
using DapperExtensions.SqlMapper.Adapters;
using DapperExtensions.SqlMapper.Cache;
using DapperExtensions.SqlMapper.Proxy;

namespace DapperExtensions
{
    /// <summary>
    /// IDbConnectionExtensions is a set of extension methods and supporting code to extend an existing IDbConnection by providing object mapping,
    /// generic interfaces,
    ///
    /// Adapted from and uses source from:
    ///     https://github.com/StackExchange/dapper-dot-net
    ///     https://github.com/StackExchange/dapper-dot-net/blob/master/Dapper%20NET40/SqlMapper.cs
    ///     https://github.com/StackExchange/dapper-dot-net/blob/master/Dapper.Contrib/SqlMapperExtensions.cs
    ///
    /// Licensed under Apache 2.0:
    ///
    ///     https://github.com/StackExchange/dapper-dot-net/blob/master/License.txt
    /// </summary>
    public static class IDbConnectionExtensions
    {
        /// <summary>
        /// The sql adapter dictionary
        ///
        /// Add any custom or updated ISqlAdapter implementation mappings here before executed mapper.
        /// </summary>
        public static readonly ConcurrentDictionary<string, ISqlAdapter> AdapterDictionary;

        private static readonly ConcurrentDictionary<RuntimeTypeHandle, PropertyInfoCacheData> _cachedComputedProperties;
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, PropertyInfoCacheData> _cachedKeyProperties;
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, StringCacheData> _cachedQueries;
        private static readonly object _cachedSweeperLock = new object();
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, PropertyInfoCacheData> _cachedTypeProperties;
        private static readonly ConcurrentDictionary<RuntimeTypeHandle, StringCacheData> _cachedTypeTableNames;

        private static readonly Timer _cacheSweeper;
        private static int _cacheExpirationMinutes = 15;
        private static int _cacheSweeperIntervalMinutes = 5;

        /// <summary>
        /// Initializes the <see cref="IDbConnectionExtensions"/> class.
        /// </summary>
        static IDbConnectionExtensions()
        {
            // Initialize Adapter Dictionary and defaults
            AdapterDictionary = new ConcurrentDictionary<string, ISqlAdapter>();

            AdapterDictionary.TryAdd("npgsqlconnection", new PostgresAdapter());
            AdapterDictionary.TryAdd("oracleconnection", new OracleAdapter());
            AdapterDictionary.TryAdd("sqlconnection", new SqlServerAdapter());
            AdapterDictionary.TryAdd("sqliteconnection", new SQLiteAdapter());

            // Initialize CachedComputedProperties Dictionary
            _cachedComputedProperties = new ConcurrentDictionary<RuntimeTypeHandle, PropertyInfoCacheData>();

            // Initialize CachedQueries Dictionary
            _cachedQueries = new ConcurrentDictionary<RuntimeTypeHandle, StringCacheData>();

            // Initialize CachedKeyProperties Dictionary
            _cachedKeyProperties = new ConcurrentDictionary<RuntimeTypeHandle, PropertyInfoCacheData>();

            // Initialize CachedTypeProperties Dictionary
            _cachedTypeProperties = new ConcurrentDictionary<RuntimeTypeHandle, PropertyInfoCacheData>();

            // Initialize CachedTypeTableNames Dictionary
            _cachedTypeTableNames = new ConcurrentDictionary<RuntimeTypeHandle, StringCacheData>();

            // Initialize CacheSweeper, but leave it disabled on not configured, first call will finalize
            _cacheSweeper = new Timer();
            GC.KeepAlive(_cacheSweeper); // THIS IS IMPORTANT -- don't let GC release timer
        }

        /// <summary>
        /// Gets the cache expiration date in ticks.
        /// </summary>
        /// <value>
        /// The cache expiration date in ticks.
        /// </value>
        public static long CacheExpirationTicks
        {
            get
            {
                return DateTime.Now.AddMinutes(_cacheExpirationMinutes * -1).Ticks;
            }
        }

        /// <summary>
        /// Handles the Elapsed event of the CacheSweeper Timer.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="ElapsedEventArgs"/> instance containing the event data.</param>
        private static void CacheSweeper_Elapsed(object sender, ElapsedEventArgs e)
        {
            // ComputedProperties
            if (_cachedComputedProperties != null && _cachedComputedProperties.Count > 0)
            {
                var computed = _cachedComputedProperties.Where(c => c.Value.LastAccess < CacheExpirationTicks);
                foreach (var item in computed)
                {
                    PropertyInfoCacheData removed;
                    _cachedComputedProperties.TryRemove(item.Key, out removed);
                }
            }

            // KeyProperties
            if (_cachedKeyProperties != null && _cachedKeyProperties.Count > 0)
            {
                var keys = _cachedKeyProperties.Where(c => c.Value.LastAccess < CacheExpirationTicks);
                foreach (var item in keys)
                {
                    PropertyInfoCacheData removed;
                    _cachedKeyProperties.TryRemove(item.Key, out removed);
                }
            }

            // Queries
            if (_cachedQueries != null && _cachedQueries.Count > 0)
            {
                var queries = _cachedQueries.Where(c => c.Value.LastAccess < CacheExpirationTicks);
                foreach (var item in queries)
                {
                    StringCacheData removed;
                    _cachedQueries.TryRemove(item.Key, out removed);
                }
            }

            // TypeProperties
            if (_cachedTypeProperties != null && _cachedTypeProperties.Count > 0)
            {
                var types = _cachedTypeProperties.Where(c => c.Value.LastAccess < CacheExpirationTicks);
                foreach (var item in types)
                {
                    PropertyInfoCacheData removed;
                    _cachedTypeProperties.TryRemove(item.Key, out removed);
                }
            }

            // TypeTableNames
            if (_cachedTypeTableNames != null && _cachedTypeTableNames.Count > 0)
            {
                var typeTables = _cachedTypeTableNames.Where(c => c.Value.LastAccess < CacheExpirationTicks);
                foreach (var item in typeTables)
                {
                    StringCacheData removed;
                    _cachedTypeTableNames.TryRemove(item.Key, out removed);
                }
            }

            // Clear SqlMapper cache
            Dapper.SqlMapper.PurgeQueryCache();
        }

        #region CRUD

        /// <summary>
        /// Delete entity in table "T".
        /// </summary>
        /// <typeparam name="T">Type of entity</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToDelete">Entity to delete</param>
        /// <returns>true if deleted, false if not found</returns>
        public static bool Delete<T>(this IDbConnection connection, T entityToDelete, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            // Argument Validation
            if (connection == null) throw new ArgumentNullException("connection", "connection cannot be null");
            if (entityToDelete == null) throw new ArgumentNullException("entityToDelete", "entityToDelete cannot be null");

            InitializeCacheSweeper();

            var type = typeof(T);

            // Get Keys
            var keyProperties = KeyPropertiesCache(type);
            if (keyProperties.Count() == 0) throw new ArgumentException("Entity must have at least one [Key] property");

            // Get TableName
            var name = GetTableName(type);

            // Build base sql
            var sb = new StringBuilder();
            sb.AppendFormat("delete from {0} where ", name);

            // Build parameters
            for (var i = 0; i < keyProperties.Count(); i++)
            {
                var property = keyProperties.ElementAt(i);
                sb.AppendFormat("{0} = @{1}", property.Name, property.Name);
                if (i < keyProperties.Count() - 1) sb.AppendFormat(" and ");
            }

            // Execute
            ISqlAdapter adapter = GetFormatter(connection);
            return adapter.Delete(connection, transaction, commandTimeout, name, keyProperties, entityToDelete);
        }

        /// <summary>
        /// Returns a single entity by a single id from table "T".
        /// If T is an interface a proxy is generated.
        /// Id must be marked with [Key] attribute.
        /// Entity is tracked/intercepted for changes and used by the Update() extension.
        /// </summary>
        /// <typeparam name="T">Entity Type</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="id">Id of the entity to get, must be marked with [Key] attribute</param>
        /// <returns>Entity of T</returns>
        public static T Get<T>(this IDbConnection connection, dynamic id, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            // Argument Validation
            if (connection == null) throw new ArgumentNullException("connection", "connection cannot be null");
            if (id == null) throw new ArgumentNullException("id", "id cannot be null");

            InitializeCacheSweeper();

            var type = typeof(T);

            StringCacheData cacheData;

            // Retrieve cached query
            if (!_cachedQueries.TryGetValue(type.TypeHandle, out cacheData))
            {
                var keys = KeyPropertiesCache(type);
                if (keys.Count() > 1) throw new DataException("Get<T> only supports an entity with a single [Key] property");
                if (keys.Count() == 0) throw new DataException("Get<T> only supports an entity with a [Key] property");

                var onlyKey = keys.First();

                var name = GetTableName(type);

                // TODO: query information schema and only select fields that are both in information schema and underlying class / interface
                var sql = "select * from " + name + " where " + onlyKey.Name + " = @id";

                cacheData = new StringCacheData(sql);
                _cachedQueries.TryAdd(type.TypeHandle, cacheData);
            }

            // Build parameters
            var dynParms = new Dapper.DynamicParameters();
            dynParms.Add("@id", id);

            T obj = null;

            // Processing
            //if (type.IsInterface) // Create proxy if interface
            //{
            //    // Execute
            //    var res = connection.Query(cacheData.Data, dynParms).SingleOrDefault() as IDictionary<string, object>;
            //    if (res == null) return (T)((object)null);

            //    // Create proxy
            //    obj = ProxyGenerator.GetInterfaceProxy<T>();

            //    // Map properties
            //    foreach (var property in TypePropertiesCache(type))
            //    {
            //        var val = res[property.Name];
            //        property.SetValue(obj, val, null);
            //    }

            //    // Reset change tracking
            //    ((IProxy)obj).IsDirty = false;
            //}
            //else
            //{
            //obj = connection.Query<T>(cacheData.Data, dynParms, transaction: transaction, commandTimeout: commandTimeout).SingleOrDefault();
            //}

            ISqlAdapter adapter = GetFormatter(connection);
            obj = adapter.Get<T>(connection, transaction, commandTimeout, cacheData.Data, dynParms);

            return obj;
        }

        /// <summary>
        /// Inserts an entity into table "T".
        /// </summary>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToInsert">Entity to insert</param>
        public static void Insert<T>(this IDbConnection connection, T entityToInsert, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            // Argument Validation
            if (connection == null) throw new ArgumentNullException("connection", "connection cannot be null");
            if (entityToInsert == null) throw new ArgumentNullException("entityToInsert", "entityToInsert cannot be null");

            InitializeCacheSweeper();

            var type = typeof(T);

            // Get TableName
            var name = GetTableName(type);

            // Build base sql
            var sbColumnList = new StringBuilder(null);

            // Get type details
            var allProperties = TypePropertiesCache(type);
            var keyProperties = KeyPropertiesCache(type);
            var computedProperties = ComputedPropertiesCache(type);
            var allPropertiesExceptComputed = allProperties.Except(computedProperties);

            // Build column list
            for (var i = 0; i < allPropertiesExceptComputed.Count(); i++)
            {
                var property = allPropertiesExceptComputed.ElementAt(i);
                sbColumnList.AppendFormat("[{0}]", property.Name);
                if (i < allPropertiesExceptComputed.Count() - 1) sbColumnList.Append(", ");
            }

            // Build parameter list
            var sbParameterList = new StringBuilder(null);
            for (var i = 0; i < allPropertiesExceptComputed.Count(); i++)
            {
                var property = allPropertiesExceptComputed.ElementAt(i);
                sbParameterList.AppendFormat("@{0}", property.Name);
                if (i < allPropertiesExceptComputed.Count() - 1) sbParameterList.Append(", ");
            }

            // Execute
            ISqlAdapter adapter = GetFormatter(connection);
            adapter.Insert(connection, transaction, commandTimeout, name, sbColumnList.ToString(), sbParameterList.ToString(), keyProperties, entityToInsert);
        }

        /// <summary>
        /// Updates entity in table "T", checks if the entity is modified if the entity is tracked by the Get() extension.
        /// </summary>
        /// <typeparam name="T">Type to be updated</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="entityToUpdate">Entity to be updated</param>
        /// <returns>true if updated, false if not found or not modified (tracked entities)</returns>
        public static bool Update<T>(this IDbConnection connection, T entityToUpdate, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            // Argument Validation
            if (connection == null) throw new ArgumentNullException("connection", "connection cannot be null");
            if (entityToUpdate == null) throw new ArgumentNullException("entityToUpdate", "entityToUpdate cannot be null");

            var proxy = entityToUpdate as IProxy;
            if (proxy != null && !proxy.IsDirty) return false; // skip unchanged proxied entities

            InitializeCacheSweeper();

            var type = typeof(T);

            // Get Keys
            var keyProperties = KeyPropertiesCache(type);
            if (!keyProperties.Any()) throw new ArgumentException("Entity must have at least one [Key] property");

            // Get TableName
            var name = GetTableName(type);

            // Get type details
            var allProperties = TypePropertiesCache(type);
            var computedProperties = ComputedPropertiesCache(type);
            var updateableProperties = allProperties.Except(keyProperties.Union(computedProperties));

            // Execute
            ISqlAdapter adapter = GetFormatter(connection);
            return adapter.Update(connection, transaction, commandTimeout, name, keyProperties, updateableProperties, entityToUpdate);
        }

        #endregion

        #region Cache

        /// <summary>
        /// Purges the query caches.
        /// </summary>
        /// <param name="connection">The connection.</param>
        public static void PurgeQueryCaches(this IDbConnection connection)
        {
            // Clear local caches
            _cachedComputedProperties.Clear();
            _cachedKeyProperties.Clear();
            _cachedQueries.Clear();
            _cachedTypeProperties.Clear();
            _cachedTypeTableNames.Clear();

            // Clear SqlMapper cache
            Dapper.SqlMapper.PurgeQueryCache();
        }

        /// <summary>
        /// The computed properties cache.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        private static IEnumerable<PropertyInfo> ComputedPropertiesCache(Type type)
        {
            PropertyInfoCacheData pi;

            // Resolve from cache
            if (_cachedComputedProperties.TryGetValue(type.TypeHandle, out pi))
            {
                var expired = CacheExpirationTicks;
                if (pi.LastAccess < expired)
                {
                    // If expired, remove from cache
                    PropertyInfoCacheData removed;
                    _cachedComputedProperties.TryRemove(type.TypeHandle, out removed);
                }
                else
                {
                    // If not expired, update last access and return it
                    var updated = pi.Clone();
                    _cachedComputedProperties.TryUpdate(type.TypeHandle, updated, pi);

                    return pi.PropertyInfos;
                }
            }

            // Get decorated with ComputedAttribute
            var properties = TypePropertiesCache(type)
                .Where(p => p.GetCustomAttributes(true).Any(a => a is SqlMapper.Attributes.ComputedAttribute))
                .ToList();

            // Cache
            _cachedComputedProperties.TryAdd(type.TypeHandle, new PropertyInfoCacheData(properties));

            return properties;
        }

        /// <summary>
        /// The key properties cache.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        private static IEnumerable<PropertyInfo> KeyPropertiesCache(Type type)
        {
            PropertyInfoCacheData pi;
            if (_cachedKeyProperties.TryGetValue(type.TypeHandle, out pi))
            {
                var expired = CacheExpirationTicks;
                if (pi.LastAccess < expired)
                {
                    // If expired, remove from cache
                    PropertyInfoCacheData removed;
                    _cachedKeyProperties.TryRemove(type.TypeHandle, out removed);
                }
                else
                {
                    // If not expired, update last access and return it
                    var updated = pi.Clone();
                    _cachedKeyProperties.TryUpdate(type.TypeHandle, updated, pi);

                    return pi.PropertyInfos;
                }
            }

            // Get decorated with KeyAttribute
            var properties = TypePropertiesCache(type)
                .Where(p => p.GetCustomAttributes(true).Any(a => a is SqlMapper.Attributes.KeyAttribute))
                .ToList();

            // Cache
            _cachedKeyProperties.TryAdd(type.TypeHandle, new PropertyInfoCacheData(properties));

            return properties;
        }

        /// <summary>
        /// The type properties cache.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        private static IEnumerable<PropertyInfo> TypePropertiesCache(Type type)
        {
            PropertyInfoCacheData pi;
            if (_cachedTypeProperties.TryGetValue(type.TypeHandle, out pi))
            {
                var expired = CacheExpirationTicks;
                if (pi.LastAccess < expired)
                {
                    // If expired, remove from cache
                    PropertyInfoCacheData removed;
                    _cachedTypeProperties.TryRemove(type.TypeHandle, out removed);
                }
                else
                {
                    // If not expired, update last access and return it
                    var updated = pi.Clone();
                    _cachedTypeProperties.TryUpdate(type.TypeHandle, updated, pi);

                    return pi.PropertyInfos;
                }
            }

            // Get writable properties
            var properties = type.GetProperties().Where(IsWritable).ToArray();

            // Cache
            _cachedTypeProperties.TryAdd(type.TypeHandle, new PropertyInfoCacheData(properties));

            return properties;
        }

        #endregion

        #region Execute

        /// <summary>
        /// Executes the specified connection.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="sql">The SQL.</param>
        /// <param name="param">The parameter.</param>
        /// <param name="transaction">The transaction.</param>
        /// <param name="commandTimeout">The command timeout.</param>
        /// <param name="commandType">Type of the command.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentNullException">connection;connection cannot be null</exception>
        /// <exception cref="System.ApplicationException">ConnectionState must be Open, current state:  + connection.State</exception>
        public static int Execute(this IDbConnection connection, string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            // Argument Validation
            if (connection == null) throw new ArgumentNullException("connection", "connection cannot be null");

            InitializeCacheSweeper();

            // Execute
            return Dapper.SqlMapper.Execute(connection, sql, param, transaction, commandTimeout, commandType);
        }

        /// <summary>
        /// Executes the reader.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="sql">The SQL.</param>
        /// <param name="param">The parameter.</param>
        /// <param name="transaction">The transaction.</param>
        /// <param name="commandTimeout">The command timeout.</param>
        /// <param name="commandType">Type of the command.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentNullException">connection;connection cannot be null</exception>
        /// <exception cref="System.ApplicationException">ConnectionState must be Open, current state:  + connection.State</exception>
        public static IDataReader ExecuteReader(this IDbConnection connection, string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            // Argument Validation
            if (connection == null) throw new ArgumentNullException("connection", "connection cannot be null");

            InitializeCacheSweeper();

            // Execute
            return Dapper.SqlMapper.ExecuteReader(connection, sql, param, transaction, commandTimeout, commandType);
        }

        /// <summary>
        /// Executes the scalar.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="sql">The SQL.</param>
        /// <param name="param">The parameter.</param>
        /// <param name="transaction">The transaction.</param>
        /// <param name="commandTimeout">The command timeout.</param>
        /// <param name="commandType">Type of the command.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentNullException">connection;connection cannot be null</exception>
        /// <exception cref="System.ApplicationException">ConnectionState must be Open, current state:  + connection.State</exception>
        public static object ExecuteScalar(this IDbConnection connection, string sql, object param = null, IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            // Argument Validation
            if (connection == null) throw new ArgumentNullException("connection", "connection cannot be null");

            InitializeCacheSweeper();

            // Execute
            return Dapper.SqlMapper.ExecuteScalar(connection, sql, param, transaction, commandTimeout, commandType);
        }

        #endregion

        #region Helpers

        /// <summary>
        /// Gets the sql formatter.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <returns></returns>
        /// <exception cref="System.ApplicationException">SqlAdapter mapping not found, ConnectionType:  + name</exception>
        public static ISqlAdapter GetFormatter(this IDbConnection connection)
        {
            string name = connection.GetType().Name.ToLower();

            ISqlAdapter adapter = null;
            AdapterDictionary.TryGetValue(name, out adapter);
            if (adapter == null) throw new ApplicationException("SqlAdapter mapping not found, ConnectionType: " + name);

            return adapter;
        }

        /// <summary>
        /// Gets the name of the table.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        /// <exception cref="System.ApplicationException">Failed to resolve TableName for Type:  + type.FullName</exception>
        private static string GetTableName(Type type)
        {
            StringCacheData cacheData;

            if (_cachedTypeTableNames.TryGetValue(type.TypeHandle, out cacheData))
            {
                var expired = CacheExpirationTicks;
                if (cacheData.LastAccess < expired)
                {
                    // If expired, remove from cache
                    StringCacheData removed;
                    _cachedTypeTableNames.TryRemove(type.TypeHandle, out removed);
                }
                else
                {
                    // If not expired, update last access and return it
                    var updated = cacheData.Clone();
                    _cachedTypeTableNames.TryUpdate(type.TypeHandle, updated, cacheData);

                    return updated.Data;
                }
            }

            // Default table name is class name
            var name = type.Name;

            // Remove interface prefix, if any
            if (type.IsInterface && name.StartsWith("I")) name = name.Substring(1);

            // Support decoration using TableAttribute
            // NOTE: This as dynamic trick should be able to handle both our own Table-attribute as well as the one in EntityFramework
            var tableattr = type.GetCustomAttributes(false).Where(attr => attr.GetType().Name == "TableAttribute").SingleOrDefault() as dynamic;
            if (tableattr != null) name = tableattr.Name;

            // Guard against null table name
            if (string.IsNullOrWhiteSpace(name)) throw new ApplicationException("Failed to resolve TableName for Type: " + type.FullName);

            // Cache mapping
            cacheData = new StringCacheData(name);
            _cachedTypeTableNames.TryAdd(type.TypeHandle, cacheData);

            return cacheData.Data;
        }

        /// <summary>
        /// Initializes the cache sweeper.
        /// </summary>
        /// <exception cref="System.ApplicationException">CacheSweeper was null, this shouldn't happen</exception>
        private static void InitializeCacheSweeper()
        {
            // Thread isolate to prevent erroneous re-initialization
            lock (_cachedSweeperLock)
            {
                // Assure that the sweeper is never null, in case of some sort of runtime error
                if (_cacheSweeper == null) throw new ApplicationException("CacheSweeper was null, this shouldn't happen");

                // THIS IS IMPORTANT -- Ignore initialization if timer is already enabled, doubling up on Event registration will lead to memory leakage
                if (_cacheSweeper.Enabled) return;

                // Initialize CacheSweeper
                _cacheSweeper.Interval = _cacheSweeperIntervalMinutes * 60 * 1000;
                _cacheSweeper.Elapsed += CacheSweeper_Elapsed;
                _cacheSweeper.Enabled = true;
            }
        }

        /// <summary>
        /// Determines whether the specified PropertyInfo is writable.
        /// </summary>
        /// <param name="pi">The PropertyInfo.</param>
        /// <returns></returns>
        private static bool IsWritable(PropertyInfo pi)
        {
            object[] attributes = pi.GetCustomAttributes(typeof(SqlMapper.Attributes.WriteAttribute), false);

            if (attributes.Length == 1)
            {
                SqlMapper.Attributes.WriteAttribute write = (SqlMapper.Attributes.WriteAttribute)attributes[0];
                return write.Write;
            }

            return true;
        }

        #endregion

        #region Query

        /// <summary>
        /// Queries the specified connection.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="sql">The SQL.</param>
        /// <param name="param">The parameter.</param>
        /// <param name="transaction">The transaction.</param>
        /// <param name="buffered">if set to <c>true</c> [buffered].</param>
        /// <param name="commandTimeout">The command timeout.</param>
        /// <param name="commandType">Type of the command.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentNullException">connection;connection cannot be null</exception>
        /// <exception cref="System.ApplicationException">ConnectionState must be Open, current state:  + connection.State</exception>
        public static IEnumerable<dynamic> Query(this IDbConnection connection, string sql, object param = null, IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null)
        {
            // Argument Validation
            if (connection == null) throw new ArgumentNullException("connection", "connection cannot be null");

            InitializeCacheSweeper();

            // Execute
            ISqlAdapter adapter = GetFormatter(connection);
            var results = adapter.Query(connection, sql, param, transaction, buffered, commandTimeout, commandType);
            return results;
        }

        /// <summary>
        /// Queries the specified connection.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection">The connection.</param>
        /// <param name="sql">The SQL.</param>
        /// <param name="param">The parameter.</param>
        /// <param name="transaction">The transaction.</param>
        /// <param name="buffered">if set to <c>true</c> [buffered].</param>
        /// <param name="commandTimeout">The command timeout.</param>
        /// <param name="commandType">Type of the command.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentNullException">connection;connection cannot be null</exception>
        /// <exception cref="System.ApplicationException">ConnectionState must be Open, current state:  + connection.State</exception>
        public static IEnumerable<T> Query<T>(this IDbConnection connection, string sql, object param = null, IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null)
        {
            // Argument Validation
            if (connection == null) throw new ArgumentNullException("connection", "connection cannot be null");

            InitializeCacheSweeper();

            // Execute
            ISqlAdapter adapter = GetFormatter(connection);
            var results = adapter.Query<T>(connection, sql, param, transaction, buffered, commandTimeout, commandType);
            return results;
        }

        #endregion
    }
}
