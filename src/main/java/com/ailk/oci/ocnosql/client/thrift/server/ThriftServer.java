package com.ailk.oci.ocnosql.client.thrift.server;

import com.ailk.oci.ocnosql.client.jdbc.phoenix.*;
import com.ailk.oci.ocnosql.client.spi.*;
import com.ailk.oci.ocnosql.client.thrift.service.*;
import com.ailk.oci.ocnosql.client.thrift.serviceImpl.*;
import org.apache.thrift.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.slf4j.*;

/**
 * Created by IntelliJ IDEA.
 * User: lile3
 * Date: 13-11-18
 * Time: 下午2:49
 * To change this template use File | Settings | File Templates.
 */
public class ThriftServer {

    private static Logger log = LoggerFactory.getLogger(ThriftServer.class);

    static TServer server;

    public static void startServer() {
        try {
            TServerTransport serverSocket = new TServerSocket(9091);
            TProtocolFactory protFactory = new TBinaryProtocol.Factory(true, true);
            TMultiplexedProcessor processor = new TMultiplexedProcessor();
            TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverSocket);
            args.processor(processor);
            args.protocolFactory(protFactory);
            server = new TThreadPoolServer(args);
            registProcessor(processor);
            System.out.println("server is listening 9091 port.");
            server.serve();
        } catch (TTransportException e) {
            log.error("transport error : ",e);
        }
    }

    public static boolean stopServer(){
       if(server != null && server.isServing()){
           server.stop();
           return true;
       }
        return false;
    }

    private static void registProcessor(TMultiplexedProcessor processor){
            ClientAdaptor adaptor = new ClientAdaptor();
            HBaseService.Processor hbaseProcessor = new HBaseService.Processor(new HBaseServiceImpl(adaptor));
            processor.registerProcessor("HBaseService", hbaseProcessor);
            SQLService.Processor sqlProcessor = new SQLService.Processor(new SQLServiceImpl(new PhoenixJdbcHelper()));
            processor.registerProcessor("SQLService",sqlProcessor);
    }

    public static void main(String[] args) {
           ThriftServer server = new ThriftServer();
           server.startServer();
    }
}
