using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Extensions.SqlMapper.Proxy
{
    public interface IProxy
    {
        bool IsDirty { get; set; }
    }
}