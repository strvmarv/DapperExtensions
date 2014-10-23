using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DapperExtensions.SqlMapper.Cache
{
    public class StringCacheData
    {
        private string _data;

        private long _lastAccess;

        public StringCacheData(string input)
        {
            this._data = input;
            this._lastAccess = DateTime.Now.Ticks;
        }

        /// <summary>
        /// Gets the data.
        /// </summary>
        /// <value>
        /// The data.
        /// </value>
        public string Data { get { return this._data; } }

        /// <summary>
        /// Gets the last access datetime in ticks.
        /// </summary>
        /// <value>
        /// The last access datetime in ticks.
        /// </value>
        public long LastAccess { get { return this._lastAccess; } }

        /// <summary>
        /// Clones this instance and updates the LastAccess ticks.
        /// </summary>
        /// <returns></returns>
        public StringCacheData Clone()
        {
            var r = this.MemberwiseClone() as StringCacheData;

            r._lastAccess = DateTime.Now.Ticks;

            return r;
        }
    }
}