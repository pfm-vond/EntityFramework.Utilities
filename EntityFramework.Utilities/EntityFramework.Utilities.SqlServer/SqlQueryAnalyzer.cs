﻿using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Storage;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

namespace EntityFramework.Utilities.SqlServer
{
    public class SqlQueryAnalyzer
    {
        public QueryInformation Analyze<T>(IQueryable<T> query) where T : class
        {
            var fromRegex = new Regex(@"FROM (\[([^\]]+)\]\.)?\[([^\]]+)\] AS (\[[^\]]+\])", RegexOptions.IgnoreCase);

            var queryInfo = new QueryInformation();

            var str = ToSql(query);
            var match = fromRegex.Match(str);
            queryInfo.Schema = match.Groups[2].Value;
            queryInfo.Table = match.Groups[3].Value;
            queryInfo.Alias = match.Groups[4].Value;

            var i = str.IndexOf("WHERE");
            if (i > 0)
            {
                var whereClause = str.Substring(i);
                queryInfo.WhereSql = whereClause.Replace(queryInfo.Alias + ".", "");
            }
            return queryInfo;
        }

        private static readonly TypeInfo QueryCompilerTypeInfo = typeof(QueryCompiler).GetTypeInfo();

        private static readonly FieldInfo QueryCompilerField = typeof(EntityQueryProvider).GetTypeInfo().DeclaredFields.First(x => x.Name == "_queryCompiler");
        private static readonly FieldInfo QueryModelGeneratorField = typeof(QueryCompiler).GetTypeInfo().DeclaredFields.First(x => x.Name == "_queryModelGenerator");
        private static readonly FieldInfo DataBaseField = QueryCompilerTypeInfo.DeclaredFields.Single(x => x.Name == "_database");
        private static readonly PropertyInfo DatabaseDependenciesField = typeof(Database).GetTypeInfo().DeclaredProperties.Single(x => x.Name == "Dependencies");

        public static string ToSql<TEntity>(IQueryable<TEntity> query)
        {
            var queryCompiler = (QueryCompiler)QueryCompilerField.GetValue(query.Provider);
            var queryModelGenerator = (QueryModelGenerator)QueryModelGeneratorField.GetValue(queryCompiler);
            var queryModel = queryModelGenerator.ParseQuery(query.Expression);
            var database = DataBaseField.GetValue(queryCompiler);
            var databaseDependencies = (DatabaseDependencies)DatabaseDependenciesField.GetValue(database);
            var queryCompilationContext = databaseDependencies.QueryCompilationContextFactory.Create(false);
            var modelVisitor = (RelationalQueryModelVisitor)queryCompilationContext.CreateQueryModelVisitor();
            modelVisitor.CreateQueryExecutor<TEntity>(queryModel);
            var sql = modelVisitor.Queries.First().ToString();

            return sql;
        }
    }
}