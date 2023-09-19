//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//

using System;
using System.Threading.Tasks;
using Microsoft.SqlTools.JsonRpc.Utility;
using Microsoft.SqlTools.ServiceLayer.Connection.Contracts;
using Microsoft.SqlTools.ServiceLayer.QueryExecution.Contracts;
using Microsoft.SqlTools.ServiceLayer.QueryExecution.Contracts.ExecuteRequests;
using Newtonsoft.Json;

namespace Microsoft.SqlTools.JsonRpc.ExecuteQuery
{
    /// <summary>
    /// Simple JSON-RPC API sample to connect to a database, execute a query, and print the results
    /// </summary>
    public class Program
    {
        internal static async Task Main(string[] args)
        {
            // set SQLTOOLSSERVICE_EXE to location of SQL Tools Service executable
            Environment.SetEnvironmentVariable("SQLTOOLSSERVICE_EXE", @"C:\Users\jordanhenkel\workspace\sqltoolsservice\src\Microsoft.SqlTools.ServiceLayer\bin\Debug\net7.0\MicrosoftSqlToolsServiceLayer.exe");

            using (SelfCleaningTempFile queryTempFile = new SelfCleaningTempFile())
            using (var myTestHelper = new ClientHelper())
            {
                Console.WriteLine("Ready; will exec on enter");
                Console.ReadLine();

                // connect w/ constr
                var expiresOn = 1695156133;
                await myTestHelper.Connect(
                    queryTempFile.FilePath,
                    new ConnectParams
                    {
                        Connection = new ConnectionDetails()
                        {
                            AzureAccountToken = "<token>",
                            ServerName = "<server>",
                            AuthenticationType = "AzureMFA",
                            ExpiresOn = 1695156133,
                            Encrypt = "true",
                            ApplicationName = "azdata",
                        }

                    }
                );

                // If the date is past expires on, warn:
                // "The access token has expired. Please reconnect to continue working."
                if (DateTime.Now > DateTimeOffset.FromUnixTimeSeconds(expiresOn).DateTime)
                {
                    Console.WriteLine("The access token has expired. Please reconnect to continue working.");
                    return;
                }

                Console.WriteLine("Connected; executing request contextulization");
                var ctx = await myTestHelper.RequestContextualization(queryTempFile.FilePath);
                // Write result as JSON to console
                Console.WriteLine(JsonConvert.SerializeObject(ctx, Formatting.Indented));
            }

        }

        internal static async Task ExecuteQuery(string query)
        {
            // create a temporary "workspace" file
            using (SelfCleaningTempFile queryTempFile = new SelfCleaningTempFile())
            // create the client helper which wraps the client driver objects
            using (ClientHelper testHelper = new ClientHelper())
            {
                // connnection details
                ConnectParams connectParams = new ConnectParams();
                connectParams.Connection = new ConnectionDetails();
                connectParams.Connection.ServerName = "localhost";
                connectParams.Connection.DatabaseName = "master";
                connectParams.Connection.AuthenticationType = "Integrated";

                // connect to the database
                await testHelper.Connect(queryTempFile.FilePath, connectParams);

                // execute the query
                QueryCompleteParams queryComplete =
                    await testHelper.RunQuery(queryTempFile.FilePath, query);

                if (queryComplete.BatchSummaries != null && queryComplete.BatchSummaries.Length > 0)
                {
                    var batch = queryComplete.BatchSummaries[0];
                    if (batch.ResultSetSummaries != null && batch.ResultSetSummaries.Length > 0)
                    {
                        var resultSet = batch.ResultSetSummaries[0];

                        // retrive the results
                        SubsetResult querySubset = await testHelper.ExecuteSubset(
                            queryTempFile.FilePath, batch.Id,
                            resultSet.Id, 0, (int)resultSet.RowCount);

                        // print the header
                        foreach (var column in resultSet.ColumnInfo)
                        {
                            Console.Write(column.ColumnName + ", ");
                        }
                        Console.Write(Environment.NewLine);

                        // print the rows
                        foreach (var row in querySubset.ResultSubset.Rows)
                        {
                            for (int i = 0; i < resultSet.ColumnInfo.Length; ++i)
                            {
                                Console.Write(row[i].DisplayValue + ", ");
                            }
                            Console.Write(Environment.NewLine);
                        }
                    }
                }

                // close database connection
                await testHelper.Disconnect(queryTempFile.FilePath);
            }
        }
    }
}
