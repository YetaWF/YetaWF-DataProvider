/* Copyright © 2020 Softel vdm, Inc. - https://yetawf.com/Documentation/YetaWF/Licensing */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Reflection;
using YetaWF.Core.DataProvider.Attributes;
using YetaWF.Core.Models;

namespace YetaWF.DataProvider.SQLGeneric {

    public class SQLGenericGen {

        public class Database {
            public string DataSource { get; set; }
            public string Name { get; set; }
            public Database() {
                CachedTables = new List<Table>();
            }
            public List<Table> CachedTables { get; set; }
        }
        public class Table {
            public string Name { get; set; }
            public string Schema { get; set; }

            public List<Column> Columns { get; set; }
            public List<Index> Indexes { get; set; }
            public List<ForeignKey> ForeignKeys { get; set; }
            public List<Column> CachedColumns { get; set; }

            public Table() {
                Columns = new List<Column>();
                Indexes = new List<Index>();
                ForeignKeys = new List<ForeignKey>();
                CachedColumns = new List<Column>();
            }
            public bool HasColumn(string name) {
                return (from c in Columns where c.Name == name select c).FirstOrDefault() != null;
            }
        }
        
        public class SubTableInfo {
            public string Name { get; set; }
            public Type Type { get; set; }
            public PropertyInfo PropInfo { get; set; } // the container's property that holds this subtable
        }

        public class Column {
            public string Name { get; set; }
            public object DataType { get; set; }
            public bool Nullable { get; set; }
            public bool Identity { get; set; }
            public int IdentityIncrement { get; set; } // NOT AVAILABLE for existing columns
            public int IdentitySeed { get; set; } // NOT AVAILABLE for existing columns
            public int Length { get; set; }
        }
        public class ColumnComparer : IEqualityComparer<Column> {
            public bool Equals(Column x, Column y) {
                return x.Name == y.Name && x.DataType.Equals(y.DataType) && x.Identity == y.Identity && x.Length == y.Length;
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

        protected bool HasIdentity(string identityName) {
            return !string.IsNullOrWhiteSpace(identityName);
        }

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
                    } else if (pi.PropertyType == typeof(Image)) {
                        ; // nothing
                    } else if (pi.PropertyType == typeof(TimeSpan)) {
                        ; // nothing
                    } else if (SQLGenericBase.TryGetDataType(pi.PropertyType)) {
                        ; // nothing
                    } else if (pi.PropertyType.IsClass && typeof(IEnumerable).IsAssignableFrom(pi.PropertyType)) {
                        // enumerated type -> subtable
                        Type subType = pi.PropertyType.GetInterfaces().Where(t => t.IsGenericType == true && t.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                                .Select(t => t.GetGenericArguments()[0]).FirstOrDefault();
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