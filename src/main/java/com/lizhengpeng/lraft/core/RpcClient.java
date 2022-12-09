package com.lizhengpeng.lraft.core;

import com.lizhengpeng.lraft.exception.RaftCodecException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.Socket;

/**
 * 对端socket的抽象
 * @author
 */
public class RpcClient {

    private static final Logger logger = LoggerFactory.getLogger(RpcClient.class);

    public static final RpcClient NO_OP = new RpcClient(null);

    /**
     * 对端通信的实际socket对象
     */
    private Socket rpcClient;

    /**
     * 构造函数
     * @param rpcClient
     */
    public RpcClient(Socket rpcClient) {
        this.rpcClient = rpcClient;
    }

    /**
     * 发送编码后的rpc消息到通信对端
     * @param msg
     */
    public void sendMessage(Object msg) {
        if (rpcClient == null) { // 空的对象实际不做任何的操作
            return;
        }
       try {
           if (rpcClient.isClosed()) {
               logger.info("rpc client => {} is closed", this);
               return;
           }
           byte[] bytes = RaftCodec.encode(msg);
           OutputStream stream = rpcClient.getOutputStream();
           stream.write(bytes);
           stream.flush();
       } catch (RaftCodecException e) {
           logger.info("rpc client => {} encode message exception", this);
       } catch (Throwable e) {
           logger.info("rpc client exception => {}", this);
           if (rpcClient != null && !rpcClient.isClosed()) {
               try {
                   rpcClient.close();
               } catch (Exception se) {
                   // Ignore Exception
               }
           }
       }
    }

    @Override
    public String toString() {
        return "RpcClient{" +
                "rpcClient=" +
                (rpcClient.getRemoteSocketAddress() != null ? rpcClient.getRemoteSocketAddress() : "unKnow remote Address") +
                '}';
    }
}
