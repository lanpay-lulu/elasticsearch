package org.relsearch;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by lanpay on 2017/8/10.
 */
public class QueryResultClientSingle {

    public static final int PORT = 8091;
    public static final String SERVER_IP = "localhost";
    public static final int TIMEOUT = 3000;
    public static final ExecutorService threadPool = Executors.newSingleThreadExecutor();
    //public static final ExecutorService threadPool = Executors.newFixedThreadPool(1);


    private QueryResultService.Client client = null;
    private TTransport transport = null;
    private static QueryResultClientSingle instance = new QueryResultClientSingle();
    private static final Logger logger = Logger.getLogger(QueryResultClient.class);

    public void send(DocBatch docBatch) {
        threadPool.submit( () ->
            send(docBatch, 2)
        );
    }

    private synchronized void send(DocBatch docBatch, int retry) {
        logger.info("send batch="+docBatch2String(docBatch));
        try {
            client.send(docBatch);
        } catch (TException e) {
            logger.error("send exception:"+e.getLocalizedMessage());
            e.printStackTrace();
            if(retry <= 0) {
                return ;
            }
            initClient();
            this.send(docBatch, retry-1);
        }
    }

    public void done(DocBatch docBatch) {
        threadPool.submit( () ->
            done(docBatch, 2)
        );
    }

    private synchronized void done(DocBatch docBatch, int retry) {
        try {
            client.done(docBatch);
        } catch (TException e) {
            e.printStackTrace();
            if(retry <= 0) {
                return ;
            }
            initClient();
            this.done(docBatch, retry-1);
        }
    }

    private synchronized void initClient() {
        if(transport != null) {
            transport.close();
        }
        TSocket socket = new TSocket(SERVER_IP, PORT);
        socket.setTimeout(TIMEOUT);
        transport = new TFramedTransport(socket);
        TProtocol protocol = new TCompactProtocol(transport);
        try{
            transport.open();
            client = new QueryResultService.Client.Factory().getClient(protocol);
        } catch (TTransportException e) {
            e.printStackTrace();
        }
    }

    public synchronized void close() {
        if(transport!=null){
            transport.close();
        }
    }

    private QueryResultClientSingle() {
        initClient();
    }

    public static QueryResultClientSingle getInstance() { return instance; }


    public static String docBatch2String(DocBatch doc) {
        StringBuilder sb = new StringBuilder("DocBatch(");
        boolean first = true;

        sb.append("requestId:");
        if (doc.requestId == null) {
            sb.append("null");
        } else {
            sb.append(doc.requestId);
        }
        first = false;
        if (!first) sb.append(", ");
        sb.append("subqueryId:");
        sb.append(doc.subqueryId);
        first = false;
        if (!first) sb.append(", ");
        sb.append("shardId:");
        sb.append(doc.shardId);
        first = false;
        if (!first) sb.append(", ");
        sb.append("localShardNum:");
        sb.append(doc.localShardNum);
        first = false;
        if (!first) sb.append(", ");
        sb.append("docs-num:");
        if (doc.docs == null) {
            sb.append("0");
        } else {
            sb.append(""+doc.docs.size());
            //sb.append(this.docs);
        }
        first = false;
        sb.append(")");
        return sb.toString();
    }
}
