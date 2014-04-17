namespace java com.ailk.oci.ocnosql.client.thrift.bean
namespace cpp  ocnosql

enum CompareOpt {
  equal = 1,
  greater = 2,
  greaterOrEquals = 3,
  minor = 4,
  minorOrEquals = 5,
  inOpt = 6
}

enum LogicalOpt {
  andOpt = 1,
  orOpt = 2
}

struct ColumnFamily{
    1: string family,
    2: list<string> columns
}

struct ColumnFilter{
    1: string columnFamily,
    2: string columnName,
    3: CompareOpt cmpOpt, 
    4: string value
}

struct ColumnFilterList{
    1:list<ColumnFilter> criterionList,
    2:LogicalOpt logicalOpt
}

