using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Entity;
using System.Data.Entity.Core.EntityClient;
using System.Data.Entity.Core.Objects;
using System.Data.Entity.Infrastructure;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;

namespace EntityFramework.Utilities
{

    public interface IEFBatchOperationBase<TContext, T> where T : class
    {
        /// <summary>
        /// Bulk insert all items if the Provider supports it. Otherwise it will use the default insert unless Configuration.DisableDefaultFallback is set to true in which case it would throw an exception.
        /// </summary>
        /// <param name="items">The items to insert</param>
        /// <param name="connection">The DbConnection to use for the insert. Only needed when for example a profiler wraps the connection. Then you need to provide a connection of the type the provider use.</param>
        /// <param name="batchSize">The size of each batch. Default depends on the provider. SqlProvider uses 15000 as default</param>        
        void InsertAll<TEntity>(IEnumerable<TEntity> items, DbConnection connection = null, int? batchSize = null) where TEntity : class, T; 
        IEFBatchOperationFiltered<TContext, T> Where(Expression<Func<T, bool>> predicate);


        /// <summary>
        /// Bulk update all items if the Provider supports it. Otherwise it will use the default update unless Configuration.DisableDefaultFallback is set to true in which case it would throw an exception.
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="items">The items to update</param>
        /// <param name="updateSpecification">Define which columns to update</param>
        /// <param name="connection">The DbConnection to use for the insert. Only needed when for example a profiler wraps the connection. Then you need to provide a connection of the type the provider use.</param>
        /// <param name="batchSize">The size of each batch. Default depends on the provider. SqlProvider uses 15000 as default</param>
        void UpdateAll<TEntity>(IEnumerable<TEntity> items, Action<UpdateSpecification<TEntity>> updateSpecification, DbConnection connection = null, int? batchSize = null) where TEntity : class, T;
    }

    public class UpdateSpecification<T>
    {
        /// <summary>
        /// Set each column you want to update, Columns that belong to the primary key cannot be updated.
        /// </summary>
        /// <param name="properties"></param>
        /// <returns></returns>
        public UpdateSpecification<T> ColumnsToUpdate(params Expression<Func<T, object>>[] properties)
        {
            Properties = properties;
            return this;
        }

        public Expression<Func<T, object>>[] Properties { get; set; }
    }

    public interface IEFBatchOperationFiltered<TContext, T>
    {
        int Delete();
        int Update<TP>(Expression<Func<T, TP>> prop, Expression<Func<T, TP>> modifier);
    }
    public static class EFBatchOperation
    {
        public static IEFBatchOperationBase<TContext, T> For<TContext, T>(TContext context, IDbSet<T> set, IConfiguration config = null)
            where TContext : DbContext
            where T : class
        {
            return EFBatchOperation<TContext, T>.For(context, set, config ?? Configuration.Default);
        }
    }
    public class EFBatchOperation<TContext, T> : IEFBatchOperationBase<TContext, T>, IEFBatchOperationFiltered<TContext, T> 
        where T : class
        where TContext : DbContext
    {
        private readonly ObjectContext _context;
        private readonly DbContext _dbContext;
        private readonly IDbSet<T> _set;
        private Expression<Func<T, bool>> _predicate;
        private IConfiguration _configuration;

        private EFBatchOperation(TContext context, IDbSet<T> set, IConfiguration config)
        {
            _dbContext = context;
            _context = (context as IObjectContextAdapter).ObjectContext;
            _set = set;
            _configuration = config;
        }

        public static IEFBatchOperationBase<TContext, T> For(TContext context, IDbSet<T> set, IConfiguration config)
        {
            return new EFBatchOperation<TContext, T>(context, set, config);
        }

        /// <summary>
        /// Bulk insert all items if the Provider supports it. Otherwise it will use the default insert unless Configuration.DisableDefaultFallback is set to true in which case it would throw an exception.
        /// </summary>
        /// <param name="items">The items to insert</param>
        /// <param name="connection">The DbConnection to use for the insert. Only needed when for example a profiler wraps the connection. Then you need to provide a connection of the type the provider use.</param>
        /// <param name="batchSize">The size of each batch. Default depends on the provider. SqlProvider uses 15000 as default</param>
        public void InsertAll<TEntity>(IEnumerable<TEntity> items, DbConnection connection = null, int? batchSize = null) where TEntity : class, T
        {
            var con = _context.Connection as EntityConnection;
            if (con == null && connection == null)
            {
                _configuration.Log("No provider could be found because the Connection didn't implement System.Data.EntityClient.EntityConnection");
                Fallbacks.DefaultInsertAll(_context, items, _configuration);
            }

            var connectionToUse = connection ?? con.StoreConnection;
            var currentType = typeof(TEntity);
            var provider = _configuration.Providers.FirstOrDefault(p => p.CanHandle(connectionToUse));
            if (provider != null && provider.CanInsert)
            {

                var mapping = EntityFramework.Utilities.EfMappingFactory.GetMappingsForContext(_dbContext);
                var typeMapping = mapping.TypeMappings[typeof(T)];
                var tableMapping = typeMapping.TableMappings.First();

                var properties = tableMapping.PropertyMappings
                    .Where(p => currentType.IsSubclassOf(p.ForEntityType) || p.ForEntityType == currentType)
                    .Select(p => new ColumnMapping { NameInDatabase = p.ColumnName, NameOnObject = p.PropertyName }).ToList();
                if (tableMapping.TPHConfiguration != null)
                {
                    properties.Add(new ColumnMapping
                    {
                        NameInDatabase = tableMapping.TPHConfiguration.ColumnName,
                        StaticValue = tableMapping.TPHConfiguration.Mappings[typeof(TEntity)]
                    });
                }

                provider.InsertItems(items, tableMapping.Schema, tableMapping.TableName, properties, connectionToUse, batchSize);
            }
            else
            {
                _configuration.Log("Found provider: " + (provider == null ? "[]" : provider.GetType().Name) + " for " + connectionToUse.GetType().Name);
                Fallbacks.DefaultInsertAll(_context, items, _configuration);
            }
        }


        public void UpdateAll<TEntity>(IEnumerable<TEntity> items, Action<UpdateSpecification<TEntity>> updateSpecification, DbConnection connection = null, int? batchSize = null) where TEntity : class, T
        {
            var con = _context.Connection as EntityConnection;
            if (con == null && connection == null)
            {
                _configuration.Log("No provider could be found because the Connection didn't implement System.Data.EntityClient.EntityConnection");
                Fallbacks.DefaultInsertAll(_context, items, _configuration);
            }

            var connectionToUse = connection ?? con.StoreConnection;
            var currentType = typeof(TEntity);
            var provider = _configuration.Providers.FirstOrDefault(p => p.CanHandle(connectionToUse));
            if (provider != null && provider.CanBulkUpdate)
            {

                var mapping = EntityFramework.Utilities.EfMappingFactory.GetMappingsForContext(_dbContext);
                var typeMapping = mapping.TypeMappings[typeof(T)];
                var tableMapping = typeMapping.TableMappings.First();

                var properties = tableMapping.PropertyMappings
                    .Where(p => currentType.IsSubclassOf(p.ForEntityType) || p.ForEntityType == currentType)
                    .Select(p => new ColumnMapping { 
                        NameInDatabase = p.ColumnName, 
                        NameOnObject = p.PropertyName, 
                        DataType = p.DataTypeFull,
                        IsPrimaryKey = p.IsPrimaryKey
                     }).ToList();

                var spec = new UpdateSpecification<TEntity>();
                updateSpecification(spec);
                provider.UpdateItems(items, tableMapping.Schema, tableMapping.TableName, properties, connectionToUse, batchSize, spec);
            }
            else
            {
                _configuration.Log("Found provider: " + (provider == null ? "[]" : provider.GetType().Name) + " for " + connectionToUse.GetType().Name);
                Fallbacks.DefaultInsertAll(_context, items, _configuration);
            }
        }

        public IEFBatchOperationFiltered<TContext, T> Where(Expression<Func<T, bool>> predicate)
        {
            _predicate = predicate;
            return this;
        }

        public int Delete()
        {
            var con = _context.Connection as EntityConnection;
            if (con == null)
            {
                _configuration.Log("No provider could be found because the Connection didn't implement System.Data.EntityClient.EntityConnection");
                return Fallbacks.DefaultDelete(_context, _predicate, _configuration);
            }

            var provider = _configuration.Providers.FirstOrDefault(p => p.CanHandle(con.StoreConnection));
            if (provider != null && provider.CanDelete)
            {
                var set = _context.CreateObjectSet<T>();
                var query = (ObjectQuery<T>)set.Where(_predicate);
                var queryInformation = provider.GetQueryInformation<T>(query);

                var delete = provider.GetDeleteQuery(queryInformation);
                var parameters = query.Parameters.Select(p => new SqlParameter { Value = p.Value, ParameterName = p.Name }).ToArray<object>();
                return _context.ExecuteStoreCommand(delete, parameters);
            }
            else
            {
                _configuration.Log("Found provider: " + (provider == null ? "[]" : provider.GetType().Name ) + " for " + con.StoreConnection.GetType().Name);
                return Fallbacks.DefaultDelete(_context, _predicate, _configuration);
            }
        }

        public int Update<TP>(Expression<Func<T, TP>> prop, Expression<Func<T, TP>> modifier)
        {
            var con = _context.Connection as EntityConnection;
            if (con == null)
            {
                _configuration.Log("No provider could be found because the Connection didn't implement System.Data.EntityClient.EntityConnection");
                return Fallbacks.DefaultUpdate(_context, _predicate, prop, modifier, _configuration);
            }

            var provider = _configuration.Providers.FirstOrDefault(p => p.CanHandle(con.StoreConnection));
            if (provider != null && provider.CanUpdate)
            {
                var set = _context.CreateObjectSet<T>();

                var query = (ObjectQuery<T>)set.Where(_predicate);
                var queryInformation = provider.GetQueryInformation<T>(query);

                var updateExpression = ExpressionHelper.CombineExpressions<T, TP>(prop, modifier);

                var mquery = ((ObjectQuery<T>)_context.CreateObjectSet<T>().Where(updateExpression));
                var mqueryInfo = provider.GetQueryInformation<T>(mquery);

                var update = provider.GetUpdateQuery(queryInformation, mqueryInfo);
                
                var parameters = query.Parameters
                    .Concat(mquery.Parameters)
                    .Select(p => new SqlParameter { Value = p.Value, ParameterName = p.Name })
                    .ToArray<object>();

                return _context.ExecuteStoreCommand(update, parameters);
            }
            else
            {
                _configuration.Log("Found provider: " + (provider == null ? "[]" : provider.GetType().Name) + " for " + con.StoreConnection.GetType().Name);
                return Fallbacks.DefaultUpdate(_context, _predicate, prop, modifier, _configuration);
            }
        }


     
    }
}
