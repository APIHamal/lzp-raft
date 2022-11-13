package com.lizhengpeng.lraft.core;

import com.lizhengpeng.lraft.exception.RaftCodecException;
import com.lizhengpeng.lraft.exception.RaftException;
import com.lizhengpeng.lraft.request.AppendLogMsg;
import com.lizhengpeng.lraft.request.RequestVoteMsg;
import com.lizhengpeng.lraft.response.AppendLogRes;
import com.lizhengpeng.lraft.response.RequestVoteRes;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * rpc服务器的实现
 * @author lzp
 */
@Setter
public class RpcServer {

    private static final Logger logger = LoggerFactory.getLogger(RpcServer.class);

    private AtomicBoolean status = new AtomicBoolean(true);

    private TaskExecutor taskExecutor;

    private MessageHandler messageHandler;

    private RaftGroupTable raftGroupTable;

    private ServerSocket serverSocket;

    private NodeId rpcServerId;

    private Executor sendExecutor = Executors.newFixedThreadPool(16);

    /**
     * 启动rpc服务的监听器
     * @param endpoint
     */
    public void startRpcServer(Endpoint endpoint) {
        if (taskExecutor == null || messageHandler == null || raftGroupTable == null) {
            throw new RaftException("rpc server taskExecutor and messageHandler and raftGroupTable can't empty");
        }
        if (endpoint == null) {
            throw new RaftException("rpc server address error");
        }
        logger.info("rpc server config {}", endpoint);
        try {
            this.rpcServerId = endpoint.getNodeId();
            serverSocket = new ServerSocket();
            serverSocket.bind(new InetSocketAddress(endpoint.getHost(), endpoint.getPort()));
            Thread serverThread = new Thread(() -> {
                Socket client = null;
                while(status.get()) {
                    try {
                        client = serverSocket.accept();
                        byte[] message = readBytes(client.getInputStream());
                        Object resMessage = RaftCodec.decode(message);
                        if (message == null) {
                            logger.warn("received message empty");
                        }
                        // 接收到投票请求
                        if (resMessage instanceof RequestVoteMsg) {
                            RequestVoteMsg msg = (RequestVoteMsg) resMessage;
                            messageHandler.onRequestVote(NodeId.of(msg.getNodeId()), msg);
                        } else if (resMessage instanceof RequestVoteRes) {
                            // 接收到投票回应
                            messageHandler.onRequestVoteCallback((RequestVoteRes) resMessage);
                        } else if (resMessage instanceof AppendLogMsg) {
                            // 接收到心跳/日志同步
                            AppendLogMsg msg = (AppendLogMsg) resMessage;
                            messageHandler.onAppendLog(NodeId.of(msg.getLeaderId()), msg);
                        } else if (resMessage instanceof AppendLogRes) {
                            // 接收到了日志同步的消息
                            AppendLogRes res = (AppendLogRes) resMessage;
                            messageHandler.onAppendLogCallback(res);
                        }
                    } catch (RaftCodecException e) {
                        logger.info("codec message occur exception", e);
                    } catch (Exception e) {
                        logger.info("accept/read socket occur exception", e);
                    } finally {
                        if (client != null) {
                            try {
                                client.close();
                            } catch (Exception e) {
                                // Ignore Exception
                            }
                        }
                    }
                }
            }, "rpc server thread");
            serverThread.setDaemon(Boolean.TRUE); // 设置为守护线程
            serverThread.start();
        } catch (Exception e) {
            throw new RaftException("rpc server start failed", e);
        } finally {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("run hook close socket");
                RpcServer.this.stopRpcServer(); // 停止当前服务器的运行
            }));
        }
    }

    private byte[] readBytes(InputStream inputStream) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[128];
        int hasRead = -1;
        while ((hasRead = inputStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, hasRead);
        }
        return outputStream.toByteArray();
    }

    /**
     * 停止rpc的服务器
     */
    public void stopRpcServer() {
        try {
            status.set(Boolean.FALSE);
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (Exception e){
            logger.info("close rpc server exception", e);
        }
    }

    /**
     * 广播数据到节点中处当前节点的其他节点
     * @param msg
     */
    public void broadcastMsg(Object msg) {
        // 编码要发送的消息
        try {
            byte[] msgBody = RaftCodec.encode(msg);
            List<Endpoint> endpoints = raftGroupTable.getBroadcastList(rpcServerId);
            for (Endpoint endpoint : endpoints) {
                sendMsg(endpoint, msgBody); // 发送数据到对端
            }
        } catch (RaftCodecException e) {
            logger.info("broadcast codec exception", e);
        }
    }

    /**
     * 发送数据到指定的节点
     * @param nodeId
     * @param msg
     */
    public void sendMsg(NodeId nodeId, Object msg) {
        try {
            sendMsg(raftGroupTable.getEndpoint(nodeId), msg);
        } catch (Exception e) {
            logger.info("send msg failed {}", nodeId);
        }
    }

    /**
     * 发送数据到指定的节点
     * @param endpoint
     * @param msg
     */
    public void sendMsg(Endpoint endpoint, Object msg) {
        try {
            sendMsg(endpoint, RaftCodec.encode(msg));
        } catch (Exception e) {
            logger.info("send msg failed {}", endpoint);
        }
    }

    /**
     * 发送数据到指定的节点
     * @param endpoint
     * @param msg
     */
    public void sendMsg(Endpoint endpoint, byte[] msg) {
        sendExecutor.execute(() -> {
            Socket client = null;
            try {
                if (endpoint == null) {
                    throw new RaftException("send msg endpoint is null");
                }
                client = new Socket();
                client.connect(new InetSocketAddress(endpoint.getHost(), endpoint.getPort()),500);
                OutputStream outputStream = client.getOutputStream();
                outputStream.write(msg);
                outputStream.flush();
            } catch (SocketTimeoutException e) {
                logger.info("send msg timeout {}", endpoint);
            } catch (Exception e) {
                logger.info("send msg failed {}", endpoint, e);
            } finally {
                if (client != null) {
                    try {
                        client.close();
                    } catch (Exception e) {
                        // Ignore Exception
                    }
                }
            }
        });
    }

}
