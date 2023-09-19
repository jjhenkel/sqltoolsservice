//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//

#nullable disable

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.SqlTools.Hosting.Protocol;
using Microsoft.SqlTools.ServiceLayer.Connection;
using Microsoft.SqlTools.ServiceLayer.Hosting;
using Microsoft.SqlTools.ServiceLayer.Metadata.Contracts;
using Microsoft.SqlTools.ServiceLayer.Utility;
using Microsoft.SqlTools.SqlCore.Metadata;
using Microsoft.SqlTools.Utility;

namespace Microsoft.SqlTools.ServiceLayer.Metadata
{
    /// <summary>
    /// Main class for Metadata Service functionality
    /// </summary>
    public sealed class MetadataService
    {
        private static readonly Lazy<MetadataService> LazyInstance = new Lazy<MetadataService>(() => new MetadataService());

        public static MetadataService Instance => LazyInstance.Value;

        private static ConnectionService connectionService = null;

        /// <summary>
        /// Internal for testing purposes only
        /// </summary>
        internal static ConnectionService ConnectionServiceInstance
        {
            get
            {
                connectionService ??= ConnectionService.Instance;
                return connectionService;
            }

            set
            {
                connectionService = value;
            }
        }

        /// <summary>
        /// Initializes the Metadata Service instance
        /// </summary>
        /// <param name="serviceHost"></param>
        /// <param name="context"></param>
        public void InitializeService(ServiceHost serviceHost)
        {
            serviceHost.SetRequestHandler(MetadataListRequest.Type, HandleMetadataListRequest, true);
            serviceHost.SetRequestHandler(TableMetadataRequest.Type, HandleGetTableRequest, true);
            serviceHost.SetRequestHandler(ViewMetadataRequest.Type, HandleGetViewRequest, true);
            serviceHost.SetRequestHandler(ContextualizationMetadataRequest.Type, HandleContextualizationMetadataRequest, true);
        }

        /// <summary>
        /// Handle a metadata query request
        /// </summary>        
        internal async Task HandleMetadataListRequest(
            MetadataQueryParams metadataParams,
            RequestContext<MetadataQueryResult> requestContext)
        {
            Func<Task> requestHandler = async () =>
            {
                ConnectionInfo connInfo;
                MetadataService.ConnectionServiceInstance.TryFindConnection(
                    metadataParams.OwnerUri,
                    out connInfo);

                var metadata = new List<ObjectMetadata>();
                if (connInfo != null)
                {
                    using (SqlConnection sqlConn = ConnectionService.OpenSqlConnection(connInfo, "Metadata"))
                    {
                        ReadMetadata(sqlConn, metadata);
                    }
                }

                await requestContext.SendResult(new MetadataQueryResult
                {
                    Metadata = metadata.ToArray()
                });
            };

            Task task = Task.Run(async () => await requestHandler()).ContinueWithOnFaulted(async t =>
            {
                await requestContext.SendError(t.Exception.ToString());
            });
            MetadataListTask = task;
        }

        internal Task MetadataListTask { get; set; }

        /// <summary>
        /// Handle a table metadata query request
        /// </summary>        
        internal static async Task HandleGetTableRequest(
            TableMetadataParams metadataParams,
            RequestContext<TableMetadataResult> requestContext)
        {
            await HandleGetTableOrViewRequest(metadataParams, "table", requestContext);
        }

        /// <summary>
        /// Handle a view metadata query request
        /// </summary>        
        internal static async Task HandleGetViewRequest(
            TableMetadataParams metadataParams,
            RequestContext<TableMetadataResult> requestContext)
        {
            await HandleGetTableOrViewRequest(metadataParams, "view", requestContext);
        }

        /// <summary>
        /// Handle a table pr view metadata query request
        /// </summary>        
        private static async Task HandleGetTableOrViewRequest(
            TableMetadataParams metadataParams,
            string objectType,
            RequestContext<TableMetadataResult> requestContext)
        {
            ConnectionInfo connInfo;
            MetadataService.ConnectionServiceInstance.TryFindConnection(
                metadataParams.OwnerUri,
                out connInfo);

            ColumnMetadata[] metadata = null;
            if (connInfo != null)
            {
                using (SqlConnection sqlConn = ConnectionService.OpenSqlConnection(connInfo, "Metadata"))
                {
                    TableMetadata table = new SmoMetadataFactory().GetObjectMetadata(
                        sqlConn, metadataParams.Schema,
                        metadataParams.ObjectName, objectType);
                    metadata = table.Columns;
                }
            }

            await requestContext.SendResult(new TableMetadataResult
            {
                Columns = metadata
            });
        }

        /// <summary>
        /// Read metadata for the current connection
        /// </summary>
        internal static void ReadMetadata(SqlConnection sqlConn, List<ObjectMetadata> metadata)
        {
            string sql =
                @"SELECT s.name AS schema_name, o.[name] AS object_name, o.[type] AS object_type
                  FROM sys.all_objects o
                    INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
                  WHERE o.[type] IN ('P','V','U','AF','FN','IF','TF') ";

            if (!IsSystemDatabase(sqlConn.Database))
            {
                sql += @"AND o.is_ms_shipped != 1 ";
            }

            sql += @"ORDER BY object_type, schema_name, object_name";

            using (SqlCommand sqlCommand = new SqlCommand(sql, sqlConn))
            {
                using (var reader = sqlCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var schemaName = reader[0] as string;
                        var objectName = reader[1] as string;
                        var objectType = reader[2] as string;

                        MetadataType metadataType;
                        string metadataTypeName;
                        if (objectType.StartsWith("V"))
                        {
                            metadataType = MetadataType.View;
                            metadataTypeName = "View";
                        }
                        else if (objectType.StartsWith("P"))
                        {
                            metadataType = MetadataType.SProc;
                            metadataTypeName = "StoredProcedure";
                        }
                        else if (objectType == "AF" || objectType == "FN" || objectType == "IF" || objectType == "TF")
                        {
                            metadataType = MetadataType.Function;
                            metadataTypeName = "UserDefinedFunction";
                        }
                        else
                        {
                            metadataType = MetadataType.Table;
                            metadataTypeName = "Table";
                        }

                        metadata.Add(new ObjectMetadata
                        {
                            MetadataType = metadataType,
                            MetadataTypeName = metadataTypeName,
                            Schema = schemaName,
                            Name = objectName
                        });
                    }
                }
            }
        }

        internal static bool IsSystemDatabase(string database)
        {
            // compare against master for now
            return string.Compare("master", database, StringComparison.OrdinalIgnoreCase) == 0;
        }

        #region "Server Contextualization Methods"

        /// <summary>
        /// Generates the contextualization metadata object for a server. The generated context 
        /// is a hierarchy of database objects that can be used to contextualize LLM-based server
        /// features.
        /// </summary>
        /// <param name="contextualizationParams">The contextualization parameters.</param>
        internal static Task HandleContextualizationMetadataRequest(ContextualizationMetadataParams contextualizationParams,
            RequestContext<ContextualizationMetadataResult> requestContext)
        {
            _ = Task.Factory.StartNew(async () =>
            {
                await GetContextualizationMetadata(contextualizationParams, requestContext);
            },
            CancellationToken.None,
            TaskCreationOptions.None,
            TaskScheduler.Default);

            return Task.CompletedTask;
        }

        internal static async Task GetContextualizationMetadata(ContextualizationMetadataParams contextualizationParams, RequestContext<ContextualizationMetadataResult> requestContext)
        {
            MetadataService.ConnectionServiceInstance.TryFindConnection(contextualizationParams.OwnerUri, out ConnectionInfo connectionInfo);

            if (connectionInfo == null)
            {
                Logger.Error("Failed to find connection info about the server.");
                throw new Exception(SR.FailedToFindConnectionInfoAboutTheServer);
            }

            var shouldGenerateMetadata = contextualizationParams.ForceRefresh || ContextualizationMetadataTempFileStream.IsScriptTempFileUpdateNeeded(
                connectionInfo.ConnectionDetails.ServerName, contextualizationParams
            );

            if (shouldGenerateMetadata)
            {
                using (SqlConnection sqlConn = ConnectionService.OpenSqlConnection(connectionInfo, "metadata"))
                {
                    try
                    {
                        // Make a fresh root node
                        var metadataRoot = new ContextualizationMetadata
                        {
                            // Children are all the databases on the server
                            Children = GetDatabaseMetadata(contextualizationParams, sqlConn)
                        };

                        // If we are set to prune empty nodes, do so
                        if (contextualizationParams.PruneEmptyNodes)
                        {
                            PruneEmptyNodesRecursive(metadataRoot);
                        }

                        await requestContext.SendResult(new ContextualizationMetadataResult { Context = metadataRoot });

                        // Make sure the metadata cache file is kept up to date
                        ContextualizationMetadataTempFileStream.Write(
                            connectionInfo.ConnectionDetails.ServerName, contextualizationParams, metadataRoot
                        );
                    }
                    catch (Exception ex)
                    {
                        Logger.Error($"An error was encountered while generating server contextualization scripts. Error: {ex.Message}");
                        throw;
                    }
                }
            }
            else
            {
                try
                {
                    var metadataRoot = ContextualizationMetadataTempFileStream.Read(
                        connectionInfo.ConnectionDetails.ServerName, contextualizationParams
                    );

                    await requestContext.SendResult(new ContextualizationMetadataResult { Context = metadataRoot });
                }
                catch (Exception ex)
                {
                    Logger.Error($"Failed to read context from the metadata cache file. Error: {ex.Message}");
                    throw;
                }
            }
        }

        internal static List<ContextualizationMetadata> GetDatabaseMetadata(ContextualizationMetadataParams contextualizationParams, SqlConnection sqlConn)
        {
            var ignoredDatabases = new List<string> { "master" };

            // We may want to clear the ignored databases list
            // if we're disabling default exclusions
            if (contextualizationParams.DisableDefaultExclusions)
            {
                ignoredDatabases.Clear();
            }

            // On the other hand, we may have extra databases to ignore
            if (contextualizationParams.ExcludeDatabases != null)
            {
                ignoredDatabases.AddRange(contextualizationParams.ExcludeDatabases);
            }

            // Build a list of parameters for the query ( { "foo", "bar" } --> "@1, @2" )
            var ignoredDatabasesAsParams = string.Join(", ", ignoredDatabases.Select((_, i) => $"@{i + 1}"));

            return ForEachMetadataRow(
                sqlConn,
                $@"SELECT name 
                   FROM [sys].[databases] 
                   WHERE name NOT IN ({ignoredDatabasesAsParams})",
                ignoredDatabases.ToArray(),
                (databaseName, objectType) =>
                {
                    return new ContextualizationMetadata
                    {
                        Kind = ContextualizationMetadataKind.Database,
                        Name = databaseName,
                        QualifiedName = $"[{databaseName}]",
                    };
                },
                (item) => GetSchemaMetadata(contextualizationParams, sqlConn, item.Name)
            );
        }

        internal static List<ContextualizationMetadata> GetSchemaMetadata(ContextualizationMetadataParams contextualizationParams, SqlConnection sqlConn, string databaseName)
        {
            var ignoredSchemas = new List<string> {
                "sys", "guest", "INFORMATION_SCHEMA",
                // Ignore schemas that are used for database roles
                "db_accessadmin", "db_backupoperator",
                "db_datareader", "db_datawriter",
                "db_ddladmin", "db_denydatareader",
                "db_denydatawriter", "db_securityadmin",
                "db_owner",
            };

            // We may want to clear the ignored schemas list 
            // if we're disabling default exclusions
            if (contextualizationParams.DisableDefaultExclusions)
            {
                ignoredSchemas.Clear();
            }

            // On the other hand, we may have extra schemas to ignore
            if (contextualizationParams.ExcludeSchemas != null)
            {
                ignoredSchemas.AddRange(contextualizationParams.ExcludeSchemas);
            }

            // Build a list of parameters for the query ( { "foo", "bar" } --> "@1, @2" )
            var ignoredSchemasAsParams = string.Join(", ", ignoredSchemas.Select((_, i) => $"@{i + 1}"));

            var sqlBuilder = new SqlCommandBuilder();
            return ForEachMetadataRow(
                sqlConn,
                $@"SELECT name 
                   FROM {sqlBuilder.QuoteIdentifier(databaseName)}.[sys].[schemas] 
                   WHERE name NOT IN ({ignoredSchemasAsParams})
                ".Trim(),
                ignoredSchemas.ToArray(),
                (schemaName, objectType) =>
                {
                    return new ContextualizationMetadata
                    {
                        Kind = ContextualizationMetadataKind.Schema,
                        Name = schemaName,
                        QualifiedName = $"[{databaseName}].[{schemaName}]",
                    };
                },
                (item) => GetTableAndViewMetadata(contextualizationParams, sqlConn, databaseName, item.Name)
            );
        }

        internal static List<ContextualizationMetadata> GetTableAndViewMetadata(ContextualizationMetadataParams contextualizationParams, SqlConnection sqlConn, string databaseName, string schemaName)
        {
            var ignoredTables = contextualizationParams.ExcludeTables?.ToList() ?? new List<string>();
            var ignoredViews = contextualizationParams.ExcludeViews?.ToList() ?? new List<string>();

            // Offset the parameter index by 2 because we have a parameter 
            // (schemaName) before the ignored tables/views
            var offset = 2;
            var ignoredTablesAsParams = string.Join(", ", ignoredTables.SelectMany(
                (_, i) => new[] { $"@{i + offset}" }).ToArray());
            var ignoredViewsAsParams = string.Join(", ", ignoredViews.SelectMany(
                (_, i) => new[] { $"@{i + ignoredTables.Count + offset}" }).ToArray());

            var sqlBuilder = new SqlCommandBuilder();
            return ForEachMetadataRow(
                sqlConn,
                $@"SELECT name, type_desc
                   FROM {sqlBuilder.QuoteIdentifier(databaseName)}.[sys].[objects]
                   WHERE schema_id = SCHEMA_ID(@1) 
                     AND (
                        (type = 'U' AND name NOT IN ({ignoredTablesAsParams}))
                        OR (type = 'V' AND name NOT IN ({ignoredViewsAsParams}))
                     )
                ".Trim(),
                (new string[] { schemaName }).Concat(ignoredTables).ToArray(),
                (objectName, objectType) =>
                {
                    return new ContextualizationMetadata
                    {
                        Kind = objectType == "USER_TABLE" ? ContextualizationMetadataKind.Table : ContextualizationMetadataKind.View,
                        Name = objectName,
                        QualifiedName = $"[{databaseName}].[{schemaName}].[{objectName}]",
                    };
                },
                (item) => GetColumnMetadata(sqlConn, databaseName, schemaName, item.Name).Concat(
                    GetForeignKeyMetadata(sqlConn, databaseName, schemaName, item.Name)
                ).ToList()
            );
        }

        internal static List<ContextualizationMetadata> GetColumnMetadata(SqlConnection sqlConn, string databaseName, string schemaName, string tableOrViewName)
        {
            var sqlBuilder = new SqlCommandBuilder();
            var quotedDbName = sqlBuilder.QuoteIdentifier(databaseName);
            return ForEachMetadataRow(
                sqlConn,
                $@"SELECT c.name, t.name
                   FROM {quotedDbName}.[sys].[columns] c
                   JOIN {quotedDbName}.[sys].[types] t 
                     ON c.system_type_id = t.system_type_id
                   JOIN {quotedDbName}.[sys].[tables] tbl 
                     ON c.object_id = tbl.object_id
                   JOIN {quotedDbName}.[sys].[schemas] s 
                     ON tbl.schema_id = s.schema_id
                   WHERE c.object_id = OBJECT_ID(@1) AND s.name = @2
                ".Trim(),
                new string[] { schemaName, tableOrViewName },
                (columnName, columnType) =>
                {
                    return new ContextualizationMetadata
                    {
                        Kind = ContextualizationMetadataKind.Column,
                        Name = columnName,
                        QualifiedName = $"[{schemaName}].[{tableOrViewName}].[{columnName}]",
                        Children = new List<ContextualizationMetadata>() {
                            new ContextualizationMetadata {
                                Kind = ContextualizationMetadataKind.ColumnType,
                                Name = columnType,
                                QualifiedName = columnType,
                            }
                        }
                    };
                }
            );
        }

        internal static List<ContextualizationMetadata> GetForeignKeyMetadata(SqlConnection sqlConn, string databaseName, string schemaName, string tableOrViewName)
        {
            var foreignKeyMetadata = new List<ContextualizationMetadata>();

            var sqlBuilder = new SqlCommandBuilder();
            var quotedDbName = sqlBuilder.QuoteIdentifier(databaseName);
            var quotedSchemaName = sqlBuilder.QuoteIdentifier(schemaName);
            var quotedTableOrViewName = sqlBuilder.QuoteIdentifier(tableOrViewName);

            var sql = $@"
                SELECT
                    fk.name AS ForeignKeyName,
                    OBJECT_NAME(fk.parent_object_id) AS TableName,
                    COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName,
                    OBJECT_NAME(fk.referenced_object_id) AS ReferencedTableName,
                    COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS ReferencedColumnName
                FROM
                    {quotedDbName}.sys.foreign_keys fk
                    INNER JOIN {quotedDbName}.sys.foreign_key_columns fc ON fk.object_id = fc.constraint_object_id
                WHERE
                    OBJECT_NAME(fk.parent_object_id) = {quotedTableOrViewName}
                    AND SCHEMA_NAME(OBJECTPROPERTY(fk.parent_object_id, 'SchemaId')) = {quotedSchemaName}
            ";

            try
            {
                using (var cmd = new SqlCommand(sql, sqlConn))
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var foreignKeyName = reader.GetString(0);
                            var tableName = reader.GetString(1);
                            var columnName = reader.GetString(2);
                            var referencedTableName = reader.GetString(3);
                            var referencedColumnName = reader.GetString(4);

                            var metadata = new ContextualizationMetadata
                            {
                                Kind = ContextualizationMetadataKind.ForeignKey,
                                Name = foreignKeyName,
                                QualifiedName = $"[{databaseName}].[{schemaName}].[{foreignKeyName}]",
                                ExtraProperties = new Dictionary<string, string>
                                {
                                    { "TableName", tableName },
                                    { "ColumnName", columnName },
                                    { "ReferencedTableName", referencedTableName },
                                    { "ReferencedColumnName", referencedColumnName }
                                }
                            };

                            foreignKeyMetadata.Add(metadata);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"An error was encountered while reading foreign key metadata. Error: {ex.Message}");
                throw;
            }

            return foreignKeyMetadata;
        }

        /// <summary>
        /// Executes a SQL query and calls a row handler for each row in the result set.
        /// This method is used at each level of gathering contextualization metadata.
        /// </summary>
        /// <param name="sqlConn">The SqlConnection to use for executing the query.</param>
        /// <param name="sql">The SQL query to execute.</param>
        /// <param name="parameters">The parameters to pass to the SQL query.</param>
        /// <param name="rowHandler">A function that takes two string arguments and returns 
        /// a ContextualizationMetadata object.</param>
        /// <param name="childMetadataGetter">A function that takes a ContextualizationMetadata
        /// object and returns a list of child ContextualizationMetadata objects.</param>
        /// <returns>A list of ContextualizationMetadata objects.</returns>
        internal static List<ContextualizationMetadata> ForEachMetadataRow(
            SqlConnection sqlConn,
            string sql,
            string[] parameters,
            Func<string, string, ContextualizationMetadata> rowHandler,
            Func<ContextualizationMetadata, List<ContextualizationMetadata>> childMetadataGetter = null
        )
        {
            var metadata = new List<ContextualizationMetadata>();

            try
            {
                using (SqlCommand sqlCommand = new SqlCommand(sql, sqlConn))
                {
                    // Add parameters to the query (would be nice to use names, but
                    // I didn't want to get into reflection)
                    for (int i = 0; i < parameters.Length; i++)
                    {
                        sqlCommand.Parameters.AddWithValue($"@{i + 1}", parameters[i]);
                    }

                    using (var reader = sqlCommand.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            // We just end up needing only 1 or 2 columns from the result set
                            // so this is a bit ugly, but functional.
                            var arg1 = reader[0] as string;
                            var arg2 = reader.FieldCount > 1 ? reader[1] as string : null;

                            var metadataItem = rowHandler(arg1, arg2);
                            metadata.Add(metadataItem);
                        }
                    }
                }

                // Now, if we have a child metadata getter, call it for each item
                // to produce the children of each item.
                if (childMetadataGetter != null)
                {
                    foreach (var item in metadata)
                    {
                        item.Children = childMetadataGetter(item);
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"An error was encountered while reading metadata. Error: {ex.Message}");
                throw;
            }

            return metadata;
        }

        internal static void PruneEmptyNodesRecursive(ContextualizationMetadata node)
        {
            // We need to prune any empty view, table, schema, or database nodes
            // from the tree.
            var nodeKindsToPrune = new List<ContextualizationMetadataKind> {
                ContextualizationMetadataKind.Database,
                ContextualizationMetadataKind.Schema,
                ContextualizationMetadataKind.Table,
                ContextualizationMetadataKind.View,
            };

            // If the node is null, return
            if (node == null)
            {
                return;
            }

            // Recursively prune child nodes
            for (int i = node.Children.Count - 1; i >= 0; i--)
            {
                var child = node.Children[i];
                PruneEmptyNodesRecursive(child);

                // If the node is one of the node kinds to prune and has no children, remove it
                if (nodeKindsToPrune.Contains(child.Kind) && child.Children.Count == 0)
                {
                    node.Children.RemoveAt(i);
                }
            }
        }

        #endregion
    }
}
