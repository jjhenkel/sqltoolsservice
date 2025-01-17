﻿//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//

using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlTools.ServiceLayer.Connection;
using Microsoft.SqlTools.ServiceLayer.Management;
using Microsoft.SqlTools.ServiceLayer.ObjectManagement.Contracts;
using Microsoft.SqlTools.ServiceLayer.ObjectManagement.ObjectTypes.Server;
using Microsoft.SqlTools.ServiceLayer.ServerConfigurations;
using Microsoft.SqlTools.ServiceLayer.Utility;

namespace Microsoft.SqlTools.ServiceLayer.ObjectManagement
{
    /// <summary>
    /// Server object type handler
    /// </summary>
    public class ServerHandler : ObjectTypeHandler<ServerInfo, ServerViewContext>
    {
        private ServerViewInfo serverViewInfo = new ServerViewInfo();
        private ServerConfigService configService = new ServerConfigService();

        public ServerHandler(ConnectionService connectionService) : base(connectionService)
        {
        }

        public override bool CanHandleType(SqlObjectType objectType)
        {
            return objectType == SqlObjectType.Server;
        }

        public override Task<InitializeViewResult> InitializeObjectView(InitializeViewRequestParams requestParams)
        {
            ConnectionInfo connInfo = this.GetConnectionInfo(requestParams.ConnectionUri);
            CDataContainer dataContainer = CDataContainer.CreateDataContainer(connInfo, databaseExists: true);

            ServerPrototype prototype = new ServerPrototype(dataContainer);

            if (prototype != null)
            {
                this.serverViewInfo.ObjectInfo = new ServerInfo()
                {
                    Name = prototype.Name,
                    HardwareGeneration = prototype.HardwareGeneration,
                    Language = prototype.Language,
                    MemoryInMB = prototype.MemoryInMB,
                    OperatingSystem = prototype.OperatingSystem,
                    Platform = prototype.Platform,
                    Processors = prototype.Processors,
                    IsClustered = prototype.IsClustered,
                    IsHadrEnabled = prototype.IsHadrEnabled,
                    IsPolyBaseInstalled = prototype.IsPolyBaseInstalled,
                    IsXTPSupported = prototype.IsXTPSupported,
                    Product = prototype.Product,
                    ReservedStorageSizeMB = prototype.ReservedStorageSizeMB,
                    RootDirectory = prototype.RootDirectory,
                    ServerCollation = prototype.ServerCollation,
                    ServiceTier = prototype.ServiceTier,
                    StorageSpaceUsageInMB = prototype.StorageSpaceUsageInMB,
                    Version = prototype.Version,
                    MinServerMemory = prototype.MinServerMemory,
                    MaxServerMemory = prototype.MaxServerMemory,
                    AutoProcessorAffinityMaskForAll = prototype.AutoProcessorAffinityMaskForAll,
                    AutoProcessorAffinityIOMaskForAll = prototype.AutoProcessorAffinityIOMaskForAll,
                    NumaNodes = prototype.NumaNodes,
                    AuthenticationMode = prototype.AuthenticationMode,
                    LoginAuditing = prototype.LoginAuditing,
                    CheckCompressBackup = prototype.CheckCompressBackup,
                    CheckBackupChecksum = prototype.CheckBackupChecksum,
                    DataLocation = prototype.DataLocation,
                    LogLocation = prototype.LogLocation,
                    BackupLocation = prototype.BackupLocation,
                    AllowTriggerToFireOthers = prototype.AllowTriggerToFireOthers,
                    BlockedProcThreshold = prototype.BlockedProcThreshold,
                    CursorThreshold = prototype.CursorThreshold,
                    DefaultFullTextLanguage = prototype.DefaultFullTextLanguage,
                    DefaultLanguage = prototype.DefaultLanguage,
                    FullTextUpgradeOption = prototype.FullTextUpgradeOption,
                    MaxTextReplicationSize = prototype.MaxTextReplicationSize,
                    OptimizeAdHocWorkloads = prototype.OptimizeAdHocWorkloads,
                    ScanStartupProcs = prototype.ScanStartupProcs,
                    TwoDigitYearCutoff = prototype.TwoDigitYearCutoff,
                    CostThresholdParallelism = prototype.CostThresholdParallelism,
                    Locks = prototype.Locks,
                    MaxDegreeParallelism = prototype.MaxDegreeParallelism,
                    QueryWait = prototype.QueryWait
                };
                serverViewInfo.LanguageOptions = (LanguageUtils.GetDefaultLanguageOptions(dataContainer)).Select(element => element.Language.Alias).ToArray();
                serverViewInfo.FullTextUpgradeOptions = Enum.GetNames(typeof(FullTextCatalogUpgradeOption)).ToArray();
            }
            var context = new ServerViewContext(requestParams);
            return Task.FromResult(new InitializeViewResult { ViewInfo = this.serverViewInfo, Context = context });
        }

        public override Task Save(ServerViewContext context, ServerInfo obj)
        {
            UpdateServerProperties(context.Parameters, obj, RunType.RunNow);
            return Task.CompletedTask;
        }

        public override Task<string> Script(ServerViewContext context, ServerInfo obj)
        {
            var script = UpdateServerProperties(
                 context.Parameters,
                 obj,
                 RunType.ScriptToWindow);
            return Task.FromResult(script);
        }

        private string UpdateServerProperties(InitializeViewRequestParams viewParams, ServerInfo serverInfo, RunType runType)
        {
            ConnectionInfo connInfo = this.GetConnectionInfo(viewParams.ConnectionUri);

            using (var dataContainer = CDataContainer.CreateDataContainer(connInfo))
            {
                try
                {
                    ServerPrototype prototype = new ServerPrototype(dataContainer);
                    prototype.ApplyInfoToPrototype(serverInfo);
                    return ConfigureServer(dataContainer, ConfigAction.Update, runType, prototype);
                }
                finally
                {
                    dataContainer.ServerConnection.Disconnect();
                }
            }
        }

        private string ConfigureServer(CDataContainer dataContainer, ConfigAction configAction, RunType runType, ServerPrototype prototype)
        {
            using (var actions = new ServerActions(dataContainer, prototype, configAction))
            {
                string sqlScript = string.Empty;
                var executionHandler = new ExecutionHandler(actions);
                executionHandler.RunNow(runType, this);
                if (executionHandler.ExecutionResult == ExecutionMode.Failure)
                {
                    throw executionHandler.ExecutionFailureException;
                }

                if (runType == RunType.ScriptToWindow)
                {
                    sqlScript = executionHandler.ScriptTextFromLastRun;
                }
                return sqlScript;
            }
        }
    }
}