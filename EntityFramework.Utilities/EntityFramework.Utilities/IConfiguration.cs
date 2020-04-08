using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace EntityFramework.Utilities
{
    public interface IConfiguration
    {
        /// <summary>
        /// Add, Remove or replace query provider by modifing this collection
        /// </summary>
        ICollection<IQueryProvider> Providers { get; set; }

        /// <summary>
        /// Allows you to hook in a logger to see debug messages for example
        /// </summary>
        Action<string> Log { get; set; }

        /// <summary>
        /// If you want an exception to be thrown if the provider doesn't support the operation set this to true. 
        /// Otherwise it will fall back to the default EF behaviour meaning a performance penalty
        /// </summary>
        bool DisableDefaultFallback { get; set; }
    }
}
