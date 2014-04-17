include "Struct.thrift"
include "Exception.thrift"

namespace java com.ailk.oci.ocnosql.client.thrift.service
namespace cpp  ocnosql


service HBaseService{
  list<list<string>> queryByRowkeyFir(1:string rowkey,2: list<string> tableNames,3:Struct.ColumnFilter columnFilter,
                                   4:map<string,string> param) throws (1:Exception.ClientRuntimeException clientExp), 
  
  list<list<string>> queryByRowkeyFirCrList(1:string rowkey,2: list<string> tableNames,3:Struct.ColumnFilterList columnFilterList,
                                   4:map<string,string> param) throws (1:Exception.ClientRuntimeException clientExp), 

  list<list<string>> queryByRowkeySec(1:string rowkey,2: list<string> tableNames,3:Struct.ColumnFilter columnFilter,
                                   4:map<string,string> param,5:list<Struct.ColumnFamily> cf) throws (1:Exception.ClientRuntimeException clientExp), 
  
  list<list<string>> queryByRowkeySecCrList(1:string rowkey,2: list<string> tableNames,3:Struct.ColumnFilterList columnFilterList,
                                   4:map<string,string> param,5:list<Struct.ColumnFamily> cf) throws (1:Exception.ClientRuntimeException clientExp), 
  
  list<list<string>> queryByRowkeyThr(1:list<string> rowkey,2: list<string> tableNames,3:Struct.ColumnFilter columnFilter,
                                   4:map<string,string> param) throws (1:Exception.ClientRuntimeException clientExp), 
  
  list<list<string>> queryByRowkeyThrCrList(1:list<string> rowkey,2: list<string> tableNames,3:Struct.ColumnFilterList columnFilterList,
                                   4:map<string,string> param) throws (1:Exception.ClientRuntimeException clientExp), 
 
  list<list<string>> queryByRowkeyPrefixFir(1:string rowkeyPrefix,2:list<string> tableNames,3:Struct.ColumnFilter columnFilter,
                                   4:map<string,string> param) throws (1:Exception.ClientRuntimeException clientExp), 

  list<list<string>> queryByRowkeyPrefixFirCrList(1:string rowkeyPrefix,2:list<string> tableNames,3:Struct.ColumnFilterList columnFilterList,
                                   4:map<string,string> param) throws (1:Exception.ClientRuntimeException clientExp), 

  list<list<string>> queryByRowkeyPrefixSec(1:string rowkeyPrefix,2:list<string> tableNames,3:Struct.ColumnFilter columnFilter,
                                   4:map<string,string> param,5:list<Struct.ColumnFamily> cf) throws (1:Exception.ClientRuntimeException clientExp), 

  list<list<string>> queryByRowkeyPrefixSecCrList(1:string rowkeyPrefix,2:list<string> tableNames,3:Struct.ColumnFilterList columnFilterList,
                                   4:map<string,string> param,5:list<Struct.ColumnFamily> cf) throws (1:Exception.ClientRuntimeException clientExp), 

  list<list<string>> queryByRowkeyFou(1:list<string> rowkey,2:list<string> tableNames,3:Struct.ColumnFilter columnFilter,
                                   4:map<string,string> param,5:list<Struct.ColumnFamily> cf) throws (1:Exception.ClientRuntimeException clientExp), 

  list<list<string>> queryByRowkeyFouCrList(1:list<string> rowkey,2:list<string> tableNames,3:Struct.ColumnFilterList columnFilterList,
                                   4:map<string,string> param,5:list<Struct.ColumnFamily> cf) throws (1:Exception.ClientRuntimeException clientExp), 

  list<list<string>> queryByRowkeyFiv(1:string startKey,2:string stopKey 3: list<string> tableNames,4:Struct.ColumnFilter columnFilter,
                                   5:map<string,string> param) throws (1:Exception.ClientRuntimeException clientExp) ,
 
  list<list<string>> queryByRowkeyFivCrList(1:string startKey,2:string stopKey 3: list<string> tableNames,4:Struct.ColumnFilterList columnFilterList,
                                   5:map<string,string> param) throws (1:Exception.ClientRuntimeException clientExp) ,
  
  list<list<string>> queryByRowkeySix(1:string startKey,2:string stopKey 3: list<string> tableNames,4:Struct.ColumnFilter columnFilter
                                   5:map<string,string> param,6:list<Struct.ColumnFamily> cf) throws (1:Exception.ClientRuntimeException clientExp) 

  list<list<string>> queryByRowkeySixCrList(1:string startKey,2:string stopKey 3: list<string> tableNames,4:Struct.ColumnFilterList columnFilterList,
                                   5:map<string,string> param,6:list<Struct.ColumnFamily> cf) throws (1:Exception.ClientRuntimeException clientExp) 
}

service SQLService{
  i32  excuteNonQueryFir(1:string sql) throws (1:Exception.SQLException sqlException),
  i32  excuteNonQuerySec(1:string sql,2:list<string> param) throws (1:Exception.SQLException sqlException),
  void excuteNonQueryThr(1:list<string> sql,2:i32 batchSize) throws (1:Exception.SQLException sqlException),
  void  excuteNonQueryFou(1:list<string> sql,2:list<list<string>> param,3:i32 batchSize) throws (1:Exception.SQLException sqlException),
  list<map<string,string>> executeQueryRawFir(1:string sql) throws (1:Exception.SQLException sqlException),
  list<map<string,string>> executeQueryRawSec(1:string sql,2:list<string> param) throws (1:Exception.SQLException sqlException),
  list<map<string,string>> executeQueryFir(1:string sql) throws (1:Exception.SQLException sqlException),
  list<map<string,string>> executeQuerySec(1:string sql,2:list<string> param) throws (1:Exception.SQLException sqlException),
  void beginTransaction() throws (1:Exception.SQLException sqlException),
  void commitTransaction() throws (1:Exception.SQLException sqlException),
  void rollbackTransaction() throws (1:Exception.SQLException sqlException),
  void close() throws (1:Exception.SQLException sqlException)
}
