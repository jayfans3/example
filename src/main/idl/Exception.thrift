namespace java com.ailk.oci.ocnosql.client.thrift.exception
namespace cpp  ocnosql

exception ClientRuntimeException{
   1:string code,
   2:string errormessage
}

exception SQLException{
   1:string code,
   2:string errormessage
}
