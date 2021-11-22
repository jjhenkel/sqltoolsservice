//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//

using System.Collections.Generic;

namespace Microsoft.SqlTools.ServiceLayer.TableDesigner.Contracts
{
    /// <summary>
    /// Table designer's view definition, there are predefined common properties.
    /// Specify the additional properties in this class.
    /// </summary>
    public class TableDesignerView
    {
        public List<DesignerDataPropertyInfo> AdditionalTableProperties { get; set; } = new List<DesignerDataPropertyInfo>();
        public BuiltinTableOptions ColumnTableOptions { get; set; } = new BuiltinTableOptions();
        public BuiltinTableOptions ForeignKeyTableOptions { get; set; } = new BuiltinTableOptions();
        public BuiltinTableOptions CheckConstraintTableOptions { get; set; } = new BuiltinTableOptions();
    }

    public class BuiltinTableOptions
    {
        public bool ShowTable { get; set; } = true;
        public List<string> PropertiesToDisplay { get; set; } = new List<string>();
        public bool canAddRows { get; set; } = true;
        public bool canRemoveRows { get; set; } = true;
        public List<DesignerDataPropertyInfo> AdditionalProperties { get; set; } = new List<DesignerDataPropertyInfo>();
    }
}