//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//

using System;
using System.Text;
using System.IO;
using Microsoft.SqlTools.Utility;
using Microsoft.SqlTools.ServiceLayer.Metadata.Contracts;
using Newtonsoft.Json;

namespace Microsoft.SqlTools.ServiceLayer.Metadata
{
    /// <summary>
    /// This class is responsible for reading, writing, and checking the validity of 
    /// the contextualization metadata temp file.
    /// </summary>
    public static class ContextualizationMetadataTempFileStream
    {
        private const short MetadataFileExpirationInHours = 1;

        /// <summary>
        /// This method writes the passed in contextualization metadata to a temporary file.
        /// </summary>
        /// <param name="serverName">The name of the server which will go on to become the name of the file.</param>
        /// <param name="metadata">The contextualization metadata that will be written to the temporary file.</param>
        public static void Write(string serverName, ContextualizationMetadataParams contextualizationParams, ContextualizationMetadata metadata)
        {
            var deterministicFileName = GenerateDeterministicFileName(serverName, contextualizationParams);
            var tempFileName = $"{deterministicFileName}.tmp";

            try
            {
                var tempFilePath = Path.Combine(Path.GetTempPath(), tempFileName);
                using (StreamWriter sw = new StreamWriter(tempFilePath, false))
                {
                    var contentSerializer = JsonSerializer.Create();
                    contentSerializer.Serialize(sw, metadata);
                }
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to write contextualization metadata to temporary file. Error: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Reads the contextualization metadata associated with the provided server name.
        /// </summary>
        /// <param name="serverName">The name of the server to retrieve the scripts for.</param>
        /// <returns>The contextualization metadata from the file.</returns>
        public static ContextualizationMetadata Read(string serverName, ContextualizationMetadataParams contextualizationParams)
        {
            var deterministicFileName = GenerateDeterministicFileName(serverName, contextualizationParams);
            var tempFileName = $"{deterministicFileName}.json.tmp";
            var metadata = new ContextualizationMetadata();

            try
            {
                var tempFilePath = Path.Combine(Path.GetTempPath(), tempFileName);
                if (!File.Exists(tempFilePath))
                {
                    return metadata;
                }

                var json = File.ReadAllText(tempFilePath);
                metadata = JsonConvert.DeserializeObject<ContextualizationMetadata>(json);
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to read metadata from temporary file. Error: {ex.Message}");
                throw;
            }

            return metadata ?? new ContextualizationMetadata();
        }

        /// <summary>
        /// Determines if the contextualization metadata file for a server is too old and needs to be updated
        /// </summary>
        /// <param name="serverName">The name of the file associated with the given server name.</param>
        /// <returns>True: The file was created within the expiration period; False: The file needs to be created
        /// or updated because it is too old.</returns>
        public static bool IsScriptTempFileUpdateNeeded(string serverName, ContextualizationMetadataParams contextualizationParams)
        {
            var deterministicFileName = GenerateDeterministicFileName(serverName, contextualizationParams);
            var tempFileName = $"{deterministicFileName}.json.tmp";

            try
            {
                var tempFilePath = Path.Combine(Path.GetTempPath(), tempFileName);
                if (!File.Exists(tempFilePath))
                {
                    return true;
                }
                else
                {
                    /**
                     * Generated scripts don't need to be super up to date, but it's also not costly to keep
                     * them up to date. So, we'll regenerate them every few hours.
                     */
                    var lastWriteTime = File.GetLastWriteTime(tempFilePath);
                    var isUpdateNeeded = (DateTime.Now - lastWriteTime).TotalHours < MetadataFileExpirationInHours ? false : true;

                    return isUpdateNeeded;
                }
            }
            catch (Exception ex)
            {
                Logger.Warning($"Unable to determine if the script file is older than {MetadataFileExpirationInHours} hours. Error: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// We need to name the file based on a deterministic value created from the server name
        /// and the ContextualizationMetadataParams. This method creates that deterministic value.
        /// </summary>
        /// <returns>Deterministic file name</returns>
        private static string GenerateDeterministicFileName(string serverName, ContextualizationMetadataParams contextualizationParams)
        {
            // We only want to be sensitive to some properties of ContextualizationMetadataParams.
            // Specifically, we want to ignore the OwnerUri and ForceRefresh properties.
            var serializedParams = JsonConvert.SerializeObject(new
            {
                contextualizationParams.PruneEmptyNodes,
                contextualizationParams.DisableDefaultExclusions,
                contextualizationParams.ExcludeDatabases,
                contextualizationParams.ExcludeSchemas,
                contextualizationParams.ExcludeTables,
                contextualizationParams.ExcludeViews,
                // ADD the serverName
                ServerName = serverName
            });

            // We need to hash the serialized params to create a deterministic file name.
            var bytes = Encoding.UTF8.GetBytes(serializedParams);
            var hash = System.Security.Cryptography.SHA256.HashData(bytes);
            var base64Hash = Convert.ToBase64String(hash);
            return base64Hash;
        }
    }
}
