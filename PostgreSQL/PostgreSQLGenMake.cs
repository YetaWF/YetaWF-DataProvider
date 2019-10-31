/* Copyright © 2019 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System.Collections.Generic;
using System.Data;

namespace YetaWF.DataProvider.PostgreSQL {

    internal partial class PostgreSQLGen {

        public class Table {
            public string Name { get; set; }

            public List<Column> Columns { get; set; }
            public List<Index> Indexes { get; set; }
            public List<ForeignKey> ForeignKeys { get; set; }

            public Table() {
                Columns = new List<Column>();
                Indexes = new List<Index>();
                ForeignKeys = new List<ForeignKey>();
            }
        }
        public class Column {
            public string Name { get; set; }
            public SqlDbType DataType { get; set; }
            public bool Nullable { get; set; }
            public bool Identity { get; set; }
            public int IdentityIncrement { get; set; } // NOT AVAILABLE for existing columns
            public int IdentitySeed { get; set; } // NOT AVAILABLE for existing columns
            public int Length { get; set; }
        }
        public class ColumnComparer : IEqualityComparer<Column> {
            public bool Equals(Column x, Column y) {
                return x.Name == y.Name && x.DataType == y.DataType && x.Identity == y.Identity && x.Length == y.Length;
            }
            public int GetHashCode(Column obj) {
                return obj.Name.GetHashCode();
            }
        }
        public class ColumnNameComparer : IEqualityComparer<Column> {
            public bool Equals(Column x, Column y) {
                return x.Name == y.Name;
            }
            public int GetHashCode(Column obj) {
                return obj.Name.GetHashCode();
            }
        }

        public enum IndexType {
            Indexed = 0,
            PrimaryKey = 1,
            UniqueKey = 2,
        }
        public class Index {
            public string Name { get; set; }
            public List<string> IndexedColumns { get; set; } // NOT AVAILABLE for existing indexes
            public IndexType IndexType { get; set; }

            public Index() {
                IndexedColumns = new List<string>();
            }
        }
        public class IndexComparer : IEqualityComparer<Index> {
            public bool Equals(Index x, Index y) {
                return x.Name == y.Name && x.IndexType == y.IndexType;
            }
            public int GetHashCode(Index obj) {
                return obj.Name.GetHashCode();
            }
        }
        public class IndexNameComparer : IEqualityComparer<Index> {
            public bool Equals(Index x, Index y) {
                return x.Name == y.Name;
            }
            public int GetHashCode(Index obj) {
                return obj.Name.GetHashCode();
            }
        }

        public class ForeignKey {
            public string Name { get; set; }
            public List<ForeignKeyColumn> ForeignKeyColumns { get; set; } // NOT AVAILABLE for existing foreign keys
            public string ReferencedTable { get; set; } // NOT AVAILABLE for existing foreign keys

            public ForeignKey() {
                ForeignKeyColumns = new List<ForeignKeyColumn>();
            }
        }
        public class ForeignKeyComparer : IEqualityComparer<ForeignKey> {
            public bool Equals(ForeignKey x, ForeignKey y) {
                return x.Name == y.Name;
            }
            public int GetHashCode(ForeignKey obj) {
                return obj.Name.GetHashCode();
            }
        }
        public class ForeignKeyColumn {
            public string Column { get; set; }
            public string ReferencedColumn { get; set; }
        }
    }
}