//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//

using Microsoft.SqlTools.Hosting.Protocol.Contracts;

namespace Microsoft.SqlTools.ServiceLayer.Metadata.Contracts
{

    public class ContextualizationMetadataParams
    {
        /// <summary>
        /// The URI to generate and retrieve contextualization metadata for.
        /// </summary>
        public string OwnerUri { get; set; }

        /// <summary>
        /// Whether or not to force a refresh of the contextualization metadata.
        /// </summary>
        public bool ForceRefresh { get; set; }

        /// <summary>
        /// Whether or not we prune empty tables/views/schemas/databases from the 
        /// contextualization metadata.
        /// </summary>
        public bool PruneEmptyNodes { get; set; }

        /// <summary>
        /// Whether or not we disable default exclusions for the 
        /// contextualization metadata (and include everything).
        /// </summary>
        /// <remarks>
        /// If any of the Exclude* properties are set, they will
        /// override this setting.
        /// </remarks>
        public bool DisableDefaultExclusions { get; set; }

        /// <summary>
        /// Databases to exclude from the contextualization metadata.
        /// </summary>
        public string[] ExcludeDatabases { get; set; }

        /// <summary>
        /// Schemas to exclude from the contextualization metadata.
        /// </summary>
        public string[] ExcludeSchemas { get; set; }

        /// <summary>
        /// Tables to exclude from the contextualization metadata.
        /// </summary>
        public string[] ExcludeTables { get; set; }

        /// <summary>
        /// Views to exclude from the contextualization metadata.
        /// </summary>
        public string[] ExcludeViews { get; set; }
    }

    public class ContextualizationMetadataResult
    {
        /// <summary>
        /// The generated context.
        /// </summary>
        public ContextualizationMetadata Context { get; set; }
    }

    public class ContextualizationMetadataRequest
    {
        public static readonly RequestType<ContextualizationMetadataParams, ContextualizationMetadataResult> Type =
            RequestType<ContextualizationMetadataParams, ContextualizationMetadataResult>.Create("metadata/getServerContext");
    }
}
