/* Copyright © 2021 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.Models;
using YetaWF.Core.Support;

namespace YetaWF.DataProvider.SQLGeneric {

    /// <summary>
    /// The base class for SQL-style dataproviders providing base services common to SQL-style dataproviders.
    /// </summary>
    public class SQLGenericGen {

        /// <summary>
        /// Defines a database.
        /// </summary>
        /// <remarks>Tables contained within the database are cached.</remarks>
        public class Database {
            /// <summary>
            /// Contains the data source where the database is located. The contents are dependent on the SQL dataprovider used.
            /// </summary>
            public string DataSource { get; set; } = null!;
            /// <summary>
            /// The database name.
            /// </summary>
            public string Name { get; set; } = null!;
            /// <summary>
            /// Constructor.
            /// </summary>
            public Database() {
                CachedTables = new List<Table>();
            }
            /// <summary>
            /// Cached list of tables within the database.
            /// </summary>
            public List<Table> CachedTables { get; set; }
        }
        /// <summary>
        /// Defines a database table.
        /// </summary>
        public class Table {
            /// <summary>
            /// The table name.
            /// </summary>
            public string Name { get; set; } = null!;
            /// <summary>
            /// The schema of the table. The contents are dependent on the SQL dataprovider used.
            /// </summary>
            public string Schema { get; set; } = null!;

            /// <summary>
            /// List of columns within the table.
            /// </summary>
            /// <remarks>The Columns property is only valid while creating/updating database tables.</remarks>
            public List<Column> Columns { get; set; }
            /// <summary>
            /// Cached list of indexes for the table.
            /// </summary>
            /// <remarks>The Indexes property is only valid while creating/updating database tables.</remarks>
            public List<Index> Indexes { get; set; }
            /// <summary>
            /// Cached list of foreign keys for the table.
            /// </summary>
            /// <remarks>The ForeignKeys property is only valid while creating/updating database tables.</remarks>
            public List<ForeignKey> ForeignKeys { get; set; }

            /// <summary>
            /// Cached list of columns for the table.
            /// </summary>
            /// <remarks>The cached list of columns is available for regular database process (get/add/update, etc.) and is not used while creating/updating database tables.</remarks>
            public List<Column> CachedColumns { get; set; }

            /// <summary>
            /// Constructor.
            /// </summary>
            public Table() {
                Columns = new List<Column>();
                Indexes = new List<Index>();
                ForeignKeys = new List<ForeignKey>();
                CachedColumns = new List<Column>();
            }
            /// <summary>
            /// Returns whether the table has a column by the specified name.
            /// </summary>
            /// <remarks>The HasColumn method is only valid while creating/updating database tables.</remarks>
            public bool HasColumn(string name) {
                return (from c in Columns where c.Name == name select c).FirstOrDefault() != null;
            }
        }

        /// <summary>
        /// Defines a subtable.
        /// </summary>
        public class SubTableInfo {
            /// <summary>
            /// The subtable name.
            /// </summary>
            public string Name { get; set; } = null!;
            /// <summary>
            /// The type of the object represented by the subtable.
            /// </summary>
            public Type Type { get; set; } = null!;
            /// <summary>
            /// The container's property that holds this subtable.
            /// </summary>
            public PropertyInfo PropInfo { get; set; } = null!;
        }

        /// <summary>
        /// Defines a column.
        /// </summary>
        public class Column {
            /// <summary>
            /// The column name.
            /// </summary>
            public string Name { get; set; } = null!;
            /// <summary>
            /// The SQL-specific data type represented by this column. The contents are dependent on the SQL dataprovider used.
            /// </summary>
            public object DataType { get; set; } = null!;
            /// <summary>
            /// Defines whether the column is nullable.
            /// </summary>
            public bool Nullable { get; set; }
            /// <summary>
            /// Defines whether the column is an identity column.
            /// </summary>
            public bool Identity { get; set; }
            /// <summary>
            /// Defines the identity increment, if the column is an identity column.
            /// </summary>
            /// <remarks>This cannot be modified when updating tables.</remarks>
            public int IdentityIncrement { get; set; } // NOT AVAILABLE for existing columns
            /// <summary>
            /// Defines the identity seed value, if the column is an identity column.
            /// </summary>
            /// <remarks>This cannot be modified when updating tables.</remarks>
            public int IdentitySeed { get; set; }
            /// <summary>
            /// The data length of the column. The contents are dependent on the SQL dataprovider used and the data type represented by the column.
            /// </summary>
            public int Length { get; set; }
        }
        /// <summary>
        /// Used to compare two columns.
        /// </summary>
        /// <remarks>Columns are considered equal if their name, data type, identity definition and data length match.</remarks>
        public class ColumnComparer : IEqualityComparer<Column> {
            /// <summary>
            /// Returns whether the columns are equal.
            /// </summary>
            /// <param name="x">The first column.</param>
            /// <param name="y">The second column.</param>
            /// <returns>Returns true if the columns are equal, false otherwise.</returns>
            public bool Equals(Column? x, Column? y) {
                if (x == null && y == null) return true;
                if (x == null || y == null) return false;
                return x.Name == y.Name && x.DataType.Equals(y.DataType) && x.Identity == y.Identity && x.Length == y.Length;
            }
            /// <summary>
            /// Returns a hash code for the specified object.
            /// </summary>
            /// <param name="obj">The object for which a hash code is to be returned.</param>
            /// <returns>Returns a hash code for the specified object.</returns>
            public int GetHashCode(Column obj) {
                return obj.Name.GetHashCode();
            }
        }
        /// <summary>
        /// Used to compare two columns.
        /// </summary>
        /// <remarks>Columns are considered equal if their names match.</remarks>
        public class ColumnNameComparer : IEqualityComparer<Column> {
            /// <summary>
            /// Returns whether the column names are equal.
            /// </summary>
            /// <param name="x">The first column.</param>
            /// <param name="y">The second column.</param>
            /// <returns>Returns true if the column names are equal, false otherwise.</returns>
            public bool Equals(Column? x, Column? y) {
                return x?.Name == y?.Name;
            }
            /// <summary>
            /// Returns a hash code for the specified object.
            /// </summary>
            /// <param name="obj">The object for which a hash code is to be returned.</param>
            /// <returns>Returns a hash code for the specified object.</returns>
            public int GetHashCode(Column obj) {
                return obj.Name.GetHashCode();
            }
        }

        /// <summary>
        /// Defines the index type.
        /// </summary>
        public enum IndexType {
            /// <summary>
            /// An indexed column.
            /// </summary>
            Indexed = 0,
            /// <summary>
            /// A primary key column.
            /// </summary>
            PrimaryKey = 1,
            /// <summary>
            /// A unique key column.
            /// </summary>
            UniqueKey = 2,
        }
        /// <summary>
        /// Defines an index.
        /// </summary>
        public class Index {
            /// <summary>
            ///  The index name.
            /// </summary>
            public string Name { get; set; } = null!;
            /// <summary>
            /// The indexed columns.
            /// </summary>
            /// <remarks>This cannot be modified when updating tables.</remarks>
            public List<string> IndexedColumns { get; set; } // NOT AVAILABLE for existing indexes
            /// <summary>
            /// Defines the index type.
            /// </summary>
            public IndexType IndexType { get; set; }

            /// <summary>
            /// Constructor.
            /// </summary>
            public Index() {
                IndexedColumns = new List<string>();
            }
        }
        /// <summary>
        /// Used to compare two indexes.
        /// </summary>
        /// <remarks>Indexes are considered equal if their name and index type match.</remarks>
        public class IndexComparer : IEqualityComparer<Index> {
            /// <summary>
            /// Returns whether the index names are equal.
            /// </summary>
            /// <param name="x">The first index.</param>
            /// <param name="y">The second index.</param>
            /// <returns>Returns true if the index name and index type are equal, false otherwise.</returns>
            public bool Equals(Index? x, Index? y) {
                return x?.Name == y?.Name && x?.IndexType == y?.IndexType;
            }
            /// <summary>
            /// Returns a hash code for the specified object.
            /// </summary>
            /// <param name="obj">The object for which a hash code is to be returned.</param>
            /// <returns>Returns a hash code for the specified object.</returns>
            public int GetHashCode(Index obj) {
                return obj.Name.GetHashCode();
            }
        }
        /// <summary>
        /// Used to compare two indexes.
        /// </summary>
        /// <remarks>Indexes are considered equal if their names match.</remarks>
        public class IndexNameComparer : IEqualityComparer<Index> {
            /// <summary>
            /// Returns whether the index names are equal.
            /// </summary>
            /// <param name="x">The first index.</param>
            /// <param name="y">The second index.</param>
            /// <returns>Returns true if the index names are equal, false otherwise.</returns>
            public bool Equals(Index? x, Index? y) {
                return x?.Name == y?.Name;
            }
            /// <summary>
            /// Returns a hash code for the specified object.
            /// </summary>
            /// <param name="obj">The object for which a hash code is to be returned.</param>
            /// <returns>Returns a hash code for the specified object.</returns>
            public int GetHashCode(Index obj) {
                return obj.Name.GetHashCode();
            }
        }

        /// <summary>
        /// Defines a foreign key.
        /// </summary>
        public class ForeignKey {
            /// <summary>
            /// The foreign key name.
            /// </summary>
            public string Name { get; set; } = null!;
            /// <summary>
            /// The foreign key columns.
            /// </summary>
            /// <remarks>This cannot be modified when updating tables.</remarks>
            public List<ForeignKeyColumn> ForeignKeyColumns { get; set; }
            /// <summary>
            /// The table referenced by the foreign key.
            /// </summary>
            /// <remarks>This cannot be modified when updating tables.</remarks>
            public string ReferencedTable { get; set; } = null!;

            /// <summary>
            /// Constructor/.
            /// </summary>
            public ForeignKey() {
                ForeignKeyColumns = new List<ForeignKeyColumn>();
            }
        }
        /// <summary>
        /// Used to compare two foreign keys.
        /// </summary>
        /// <remarks>Foreign keys are considered equal if their names match.</remarks>
        public class ForeignKeyComparer : IEqualityComparer<ForeignKey> {
            /// <summary>
            /// Returns whether the foreign key names are equal.
            /// </summary>
            /// <param name="x">The first foreign key.</param>
            /// <param name="y">The second foreign key.</param>
            /// <returns>Returns true if the foreign key names are equal, false otherwise.</returns>
            public bool Equals(ForeignKey? x, ForeignKey? y) {
                return x?.Name == y?.Name;
            }
            /// <summary>
            /// Returns a hash code for the specified object.
            /// </summary>
            /// <param name="obj">The object for which a hash code is to be returned.</param>
            /// <returns>Returns a hash code for the specified object.</returns>
            public int GetHashCode(ForeignKey obj) {
                return obj.Name.GetHashCode();
            }
        }
        /// <summary>
        /// Defines a foreign key column.
        /// </summary>
        public class ForeignKeyColumn {
            /// <summary>
            /// The column name.
            /// </summary>
            public string Column { get; set; } = null!;
            /// <summary>
            /// The name of the referenced column.
            /// </summary>
            public string ReferencedColumn { get; set; } = null!;
        }

        /// <summary>
        /// Returns whether an identity name is available.
        /// </summary>
        /// <param name="identityName">The known identity name or null.</param>
        /// <returns>Returns true if an identity name is available, false otherwise.</returns>
        protected bool HasIdentity([NotNullWhen(true)]string? identityName) {
            return !string.IsNullOrWhiteSpace(identityName);
        }

        /// <summary>
        /// Returns a table's subtables.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="propData">The proprty data of the object describing the table.</param>
        /// <returns>Returns a list of subtables.</returns>
        public static List<SubTableInfo> GetSubTables(string tableName, List<PropertyData> propData) {
            List<SubTableInfo> list = new List<SubTableInfo>();
            foreach (PropertyData prop in propData) {
                PropertyInfo pi = prop.PropInfo;
                if (pi.CanRead && pi.CanWrite && !prop.HasAttribute("DontSave") && !prop.CalculatedProperty && !prop.HasAttribute(Data_DontSave.AttributeName)) {
                    if (prop.HasAttribute(Data_Identity.AttributeName)) {
                        ; // nothing
                    } else if (prop.HasAttribute(Data_BinaryAttribute.AttributeName)) {
                        ; // nothing
                    } else if (pi.PropertyType == typeof(MultiString)) {
                        ; // nothing
                    } else if (pi.PropertyType == typeof(System.Drawing.Image)) {
                        throw new InternalError("Image and Bitmap types no longer supported/needed");
                    } else if (pi.PropertyType == typeof(TimeSpan)) {
                        ; // nothing
                    } else if (SQLGenericBase.TryGetDataType(pi.PropertyType)) {
                        ; // nothing
                    } else if (pi.PropertyType.IsClass && typeof(IEnumerable).IsAssignableFrom(pi.PropertyType)) {
                        // enumerated type -> subtable
                        Type subType = pi.PropertyType.GetInterfaces().Where(t => t.IsGenericType == true && t.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                                .Select(t => t.GetGenericArguments()[0]).First();
                        string subTableName = tableName + "_" + pi.Name;
                        list.Add(new SubTableInfo {
                            Name = subTableName,
                            Type = subType,
                            PropInfo = pi,
                        });
                    }
                }
            }
            return list;
        }
    }
}