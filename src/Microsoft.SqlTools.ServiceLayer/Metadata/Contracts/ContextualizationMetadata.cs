//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//

using System.Collections.Generic;

namespace Microsoft.SqlTools.ServiceLayer.Metadata.Contracts
{
    /// <summary>
    /// Server Contextualization Item kind enumeration
    /// (similar to MetadataType, but a bit more granular).
    /// </summary>
    public enum ContextualizationMetadataKind
    {
        Root = 0,
        Database = 1,
        Schema = 2,
        Table = 3,
        View = 4,
        StoredProcedure = 5,
        Function = 6,
        Column = 7,
        ColumnType = 8,
        ForeignKey = 9,
    }

    /// <summary>
    /// Server Contextualization Item: a representation of 
    /// the database object hierarchy built for contextualization
    /// of LLM-based server features.
    /// </summary>
    public class ContextualizationMetadata
    {
        /// <summary>
        /// Default constructor (creates an empty root server contextualization item).
        /// </summary>
        public ContextualizationMetadata()
        {
            this.Kind = ContextualizationMetadataKind.Root;
            this.Name = "$root";
            this.QualifiedName = "$root";
            this.ExtraProperties = new Dictionary<string, string>();
            this.Children = new List<ContextualizationMetadata>();
        }

        /// <summary>
        /// The kind of contextualization item this is.
        /// </summary>
        public ContextualizationMetadataKind Kind { get; set; }

        /// <summary>
        /// The name of the kind of contextualization item this is: 
        /// "database", "schema", "table", "view", 
        /// "stored procedure", "function", "column", 
        /// "column type"
        /// </summary>
        public string KindName { get { return this.Kind.ToString(); } }

        /// <summary>
        /// The name of the contextualization item (table name, view name, type name, etc.).
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The qualified name of the contextualization item.
        /// </summary>
        public string QualifiedName { get; set; }

        /// <summary>
        /// A dictionary of extra (optional) properties for this contextualization item.
        /// </summary>
        public Dictionary<string, string> ExtraProperties { get; set; }

        /// <summary>
        /// The children of this contextualization item 
        /// (table -> column, column -> column type, etc.).
        /// </summary>
        public List<ContextualizationMetadata> Children { get; set; }
    }
}