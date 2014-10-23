using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DapperExtensions.SqlMapper.Attributes
{
    [AttributeUsage(AttributeTargets.Property)]
    public class WriteAttribute : Attribute
    {
        public WriteAttribute(bool write)
        {
            Write = write;
        }

        public bool Write { get; private set; }
    }
}