//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//

using System;
using System.Threading.Tasks;
using Microsoft.SqlTools.JsonRpc.Driver;
using Microsoft.SqlTools.ServiceLayer.Connection.Contracts;
using Microsoft.SqlTools.ServiceLayer.LanguageServices.Contracts;
using Microsoft.SqlTools.ServiceLayer.QueryExecution.Contracts;
using Microsoft.SqlTools.ServiceLayer.SqlContext;
using Microsoft.SqlTools.ServiceLayer.Workspace.Contracts;
using Microsoft.SqlTools.ServiceLayer.QueryExecution.Contracts.ExecuteRequests;
using Microsoft.SqlTools.ServiceLayer.Metadata.Contracts;

namespace Microsoft.SqlTools.JsonRpc.Utility
{
    /// <summary>
    /// Base class for all test suites run by the test driver
    /// </summary>
    public sealed class ClientHelper : IDisposable
    {
        private bool isRunning = false;

        public ClientHelper()
        {
            Driver = new ClientDriver();
            Driver.Start().Wait();
            this.isRunning = true;
        }

        public void Dispose()
        {
            if (this.isRunning)
            {
                WaitForExit();
            }
        }

        public void WaitForExit()
        {
            try
            {
                this.isRunning = false;
                Driver.Stop().Wait();
                Console.WriteLine("Successfully killed process.");
            }
            catch(Exception e)
            {
                Console.WriteLine($"Exception while waiting for service exit: {e.Message}");
            }
        }

        /// <summary>
        /// The driver object used to read/write data to the service
        /// </summary>
        public ClientDriver Driver
        {
            get;
            private set;
        }

        private object fileLock = new Object();

        /// <summary>
        /// Request a new connection to be created
        /// </summary>
        /// <returns>True if the connection completed successfully</returns>        
        public async Task<bool> Connect(string ownerUri, ConnectParams connectParams, int timeout = 15000)
        { 
            connectParams.OwnerUri = ownerUri;
            var connectResult = await Driver.SendRequest(ConnectionRequest.Type, connectParams);
            if (connectResult)
            {
                var completeEvent = await Driver.WaitForEvent(ConnectionCompleteNotification.Type, timeout);
                Console.WriteLine("Connected: " + completeEvent.ConnectionId);
                return !string.IsNullOrEmpty(completeEvent.ConnectionId);
            }
            else
            {
                Console.WriteLine("Failed to connect");
                return false;
            }
        }

        /// <summary>
        /// Request a disconnect
        /// </summary>
        public async Task<bool> Disconnect(string ownerUri)
        {
            var disconnectParams = new DisconnectParams();
            disconnectParams.OwnerUri = ownerUri;

            var disconnectResult = await Driver.SendRequest(DisconnectRequest.Type, disconnectParams);
            return disconnectResult;
        }

        /// <summary>
        /// Request a cancel connect
        /// </summary>
        public async Task<bool> CancelConnect(string ownerUri)
        {
            var cancelParams = new CancelConnectParams();
            cancelParams.OwnerUri = ownerUri;

            return await Driver.SendRequest(CancelConnectRequest.Type, cancelParams);
        }

        /// <summary>
        /// Request a cancel connect
        /// </summary>
        public async Task<ListDatabasesResponse> ListDatabases(string ownerUri)
        {
            var listParams = new ListDatabasesParams();
            listParams.OwnerUri = ownerUri;

            return await Driver.SendRequest(ListDatabasesRequest.Type, listParams);
        }

        /// <summary>
        /// Request the active SQL script is parsed for errors
        /// </summary>
        public async Task<SubsetResult> RequestQueryExecuteSubset(SubsetParams subsetParams)
        {
            return await Driver.SendRequest(SubsetRequest.Type, subsetParams);
        }

        /// <summary>
        /// Request the active SQL script is parsed for errors
        /// </summary>
        public async Task RequestOpenDocumentNotification(DidOpenTextDocumentNotification openParams)
        {
            await Driver.SendEvent(DidOpenTextDocumentNotification.Type, openParams);
        }

        /// <summary>
        /// Request a configuration change notification
        /// </summary>
        public async Task RequestChangeConfigurationNotification(DidChangeConfigurationParams<SqlToolsSettings> configParams)
        {
            await Driver.SendEvent(DidChangeConfigurationNotification<SqlToolsSettings>.Type, configParams);
        }

        /// <summary>
        /// Request metadata for a given connection
        /// </summary>
        public async Task<ContextualizationMetadata> RequestContextualization(string ownerUri)
        {
            var contextualizationParams = new ContextualizationMetadataParams();
            contextualizationParams.OwnerUri = ownerUri;
            // contextualizationParams.PruneEmptyNodes = true;

            var response = await Driver.SendRequest(ContextualizationMetadataRequest.Type, contextualizationParams);
            return response.Context;
        }

        /// <summary>
        /// /// Request the active SQL script is parsed for errors
        /// </summary>
        public async Task RequestChangeTextDocumentNotification(DidChangeTextDocumentParams changeParams)
        {
            await Driver.SendEvent(DidChangeTextDocumentNotification.Type, changeParams);
        }
        
        /// <summary>
        /// Request completion item resolve to look-up additional info
        /// </summary>
        public async Task<CompletionItem> RequestResolveCompletion(CompletionItem item)
        {
            var result = await Driver.SendRequest(CompletionResolveRequest.Type, item);
            return result;
        }

        // /// <summary>
        // /// Request a Read Credential for given credential id
        // /// </summary>
        // public async Task<Credential> ReadCredential(string credentialId)
        // {
        //     var credentialParams = new Credential();
        //     credentialParams.CredentialId = credentialId;

        //     return await Driver.SendRequest(ReadCredentialRequest.Type, credentialParams);
        // }

        /// <summary>
        /// Run a query using a given connection bound to a URI
        /// </summary>
        public async Task<QueryCompleteParams> RunQuery(string ownerUri, string query, int timeoutMilliseconds = 5000)
        {
            // Write the query text to a backing file
            WriteToFile(ownerUri, query);

            var queryParams = new ExecuteDocumentSelectionParams
            {
                OwnerUri = ownerUri,
                QuerySelection = null
            };

            var result = await Driver.SendRequest(ExecuteDocumentSelectionRequest.Type, queryParams);
            if (result != null)
            {
                var eventResult = await Driver.WaitForEvent(QueryCompleteEvent.Type, timeoutMilliseconds);
                return eventResult;
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Request to save query results as CSV
        /// </summary>
        public async Task<SaveResultRequestResult> SaveAsCsv(string ownerUri, string filename, int batchIndex, int resultSetIndex)
        {
            var saveParams = new SaveResultsAsCsvRequestParams
            {
                OwnerUri = ownerUri,
                BatchIndex = batchIndex,
                ResultSetIndex = resultSetIndex,
                FilePath = filename
            };

            var result = await Driver.SendRequest(SaveResultsAsCsvRequest.Type, saveParams);
            return result;
        }

        /// <summary>
        /// Request to save query results as JSON
        /// </summary>
        public async Task<SaveResultRequestResult> SaveAsJson(string ownerUri, string filename, int batchIndex, int resultSetIndex)
        {
            var saveParams = new SaveResultsAsJsonRequestParams
            {
                OwnerUri = ownerUri,
                BatchIndex = batchIndex,
                ResultSetIndex = resultSetIndex,
                FilePath = filename
            };

            var result = await Driver.SendRequest(SaveResultsAsJsonRequest.Type, saveParams);
            return result;
        }

        /// <summary>
        /// Request a subset of results from a query
        /// </summary>
        public async Task<SubsetResult> ExecuteSubset(string ownerUri, int batchIndex, int resultSetIndex, int rowStartIndex, int rowCount)
        {
            var subsetParams = new SubsetParams();
            subsetParams.OwnerUri = ownerUri;
            subsetParams.BatchIndex = batchIndex;
            subsetParams.ResultSetIndex = resultSetIndex;
            subsetParams.RowsStartIndex = rowStartIndex;
            subsetParams.RowsCount = rowCount;

            var result = await Driver.SendRequest(SubsetRequest.Type, subsetParams);
            return result;
        }

        public void WriteToFile(string ownerUri, string query)
        {
            lock (fileLock)
            {
                System.IO.File.WriteAllText(ownerUri, query);
            }
        }
    }
}
