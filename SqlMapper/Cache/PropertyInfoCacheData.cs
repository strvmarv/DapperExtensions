using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Extensions.SqlMapper.Cache
{
    public class PropertyInfoCacheData
    {
        private long _lastAccess;
        private List<PropertyInfo> _propertyInfos;

        public PropertyInfoCacheData(IEnumerable<PropertyInfo> input)
        {
            if (input == null) throw new ArgumentNullException("input", "input cannot be null");

            this._propertyInfos = new List<PropertyInfo>();
            this._lastAccess = DateTime.Now.Ticks;

            this._propertyInfos.AddRange(input);
        }

        /// <summary>
        /// Gets the last access datetime in ticks.
        /// </summary>
        /// <value>
        /// The last access datetime in ticks.
        /// </value>
        public long LastAccess { get { return this._lastAccess; } }

        /// <summary>
        /// Gets the property infos.
        /// </summary>
        /// <value>
        /// The property infos.
        /// </value>
        public IEnumerable<PropertyInfo> PropertyInfos { get { return this._propertyInfos; } }

        /// <summary>
        /// Clones this instance and updates the LastAccess ticks.
        /// </summary>
        /// <returns></returns>
        public PropertyInfoCacheData Clone()
        {
            var r = this.MemberwiseClone() as PropertyInfoCacheData;

            r._lastAccess = DateTime.Now.Ticks;

            return r;
        }
    }
}