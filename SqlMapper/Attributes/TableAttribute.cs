using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Extensions.SqlMapper.Attributes
{
    [AttributeUsage(AttributeTargets.Class)]
    public class TableAttribute : Attribute
    {
        public TableAttribute(string tableName)
        {
            Name = tableName;
        }

        public string Name { get; private set; }
    }
}