package com.lizhengpeng.lraft.core;

import com.lizhengpeng.lraft.exception.RaftCodecException;
import com.lizhengpeng.lraft.exception.RaftException;
import com.lizhengpeng.lraft.request.AppendLogMsg;
import com.lizhengpeng.lraft.request.ClientRequestMsg;
import com.lizhengpeng.lraft.request.RefreshLeaderMsg;
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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * rpc服务器的实现
 * @author lzp
 */
@Setter
public class RpcServer {

    private static final Logger logger = LoggerFactory.getLogger(RpcServer.class);

    private AtomicBoolean status = new AtomicBoolean(true);

    private MessageHandler messageHandler;

    private RaftGroupTable raftGroupTable;

    private ServerSocket serverSocket;

    private NodeId rpcServerId;

    private ThreadPoolExecutor ioExecutor = new ThreadPoolExecutor(8, 32, 1000 * 60, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(4096));

    private ConcurrentHashMap<NodeId, RpcClientHolder> rpcClientHolder = new ConcurrentHashMap<>();

    /**
     * 启动rpc服务的监听器
     * @param endpoint
     */
    public void startRpcServer(Endpoint endpoint) {
        if (messageHandler == null || raftGroupTable == null) {
            throw new RaftException("rpc server taskExecutor and messageHandler and raftGroupTable can't empty");
        }
        if (endpoint == null) {
            throw new RaftException("rpc server address error");
        }
        logger.info("rpc server config {}", endpoint);
        try {
            rpcServerId = endpoint.getNodeId();
            serverSocket = new ServerSocket();
            serverSocket.bind(new InetSocketAddress(endpoint.getHost(), endpoint.getPort()));
            Thread serverThread = new Thread(() -> {
                while(status.get()) {
                    try {
                        Socket rpcClient = serverSocket.accept();
                        // pre thread for connection模型保持长连接
                        // 通常一个集群的规模不会很大
                        ioExecutor.execute(() -> handlerMessage(rpcClient));
                    } catch (RaftCodecException e) {
                        logger.info("codec message occur exception", e);
                    } catch (Exception e) {
                        logger.info("accept socket occur exception", e);
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

    /**
     * 接收客户端的消息
     * @param client
     */
    private void handlerMessage(Socket client) {
        try {
            // 构造通信客户端对象
            RpcClient rpcClient = new RpcClient(client);
            // 复用连接避免使用完后直接关闭提升整体IO的性能
            // 正常情况下对端rpc连接不出现异常这个连接不会中断
            while (true) {
                // readRpcMessage方法解决了粘包的问题
                // rpc包的具体格式见RaftCodec方法
                Object resMessage = RaftCodec.decode(readRpcMessage(client.getInputStream()));
                if (resMessage == null) {
                    logger.warn("received message empty");
                    continue;
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
                } else if (resMessage instanceof ClientRequestMsg) {
                    // leader收到了来自客户端的业务操作请求
                    ClientRequestMsg clientRequestMsg = (ClientRequestMsg) resMessage;
                    messageHandler.onLeaderAppendLog(clientRequestMsg, rpcClient);
                } else if (resMessage instanceof RefreshLeaderMsg) {
                    // 客户端尝试获取leader节点
                    RefreshLeaderMsg refreshLeaderMsg = (RefreshLeaderMsg) resMessage;
                    messageHandler.onRefreshLeader(refreshLeaderMsg, rpcClient);
                }
            }
        } catch (RaftCodecException e) {
            logger.info("codec message occur exception", e);
        } catch (IOException e) {
            logger.info("remote client => {} io exception", client.getRemoteSocketAddress());
        } catch (Exception e) {
            logger.info("read message occur exception", e);
        } finally {
            if (client != null && !client.isClosed()) {
                try {
                    client.close();
                } catch (Exception e) {
                    // Ignore exception
                }
            }
        }
    }

    /**
     * 读取完整的raft消息(处理粘包和拆包的问题)
     * length(4字节表示后面内容的长度)+type(1字节)+message()
     * @param inputStream
     * @return
     * @throws IOException
     */
    public static byte[] readRpcMessage(InputStream inputStream) throws IOException {
        int messageLength = Integer.parseInt(new String(readRpcMessage(inputStream, RaftCodec.HEAD_LENGTH), StandardCharsets.UTF_8));
        return readRpcMessage(inputStream, messageLength);
    }

    /**
     * 读取指定长度的消息
     * @param inputStream
     * @return
     * @throws IOException
     */
    private static byte[] readRpcMessage(InputStream inputStream, int fixedLength) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        byte[] buffer = new byte[fixedLength];
        int hasRead, total = 0;
        while ((hasRead = inputStream.read(buffer)) != -1) {
            stream.write(buffer, 0, hasRead);
            total += hasRead;
            if (total == fixedLength) {
                break;
            } else {
                buffer = new byte[fixedLength - total];
            }
        }
        return stream.toByteArray();
    }

    /**
     * 停止rpc的服务器
     */
    public void stopRpcServer() {
        status.set(Boolean.FALSE);
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (Exception e) {
            logger.info("close server socket", e);
        }
        try {
            if (ioExecutor != null) {
                ioExecutor.shutdown();
            }
        } catch (Exception e) {
            logger.info("close io executor", e);
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
        ioExecutor.execute(() -> { // 发送消息的时候异步
            try {
                if (endpoint == null) {
                    throw new RaftException("send msg endpoint is null");
                }
                RpcClientHolder clientHolder = rpcClientHolder.get(endpoint.getNodeId());
                if (clientHolder == null) {
                    // 客户端不存在的情况下则需要新建
                    rpcClientHolder.putIfAbsent(endpoint.getNodeId(), new RpcClientHolder());
                    clientHolder = rpcClientHolder.get(endpoint.getNodeId());
                }
                try {
                    // 对同一个NodeId的连接访问进行加锁这样可以减小锁的范围
                    // 当对同一个nodeId有多个rpc的消息时可以做优化
                    // 可以临时开启多个socket对象来并发访问
                    // 但是最后只保留一个有效的socket
                    clientHolder.lock();
                    Socket rpcClient = clientHolder.getSocket();
                    if (rpcClient == null) {
                        // 创建新的客户端并且成功后进行缓存
                        // 此时则对连接进行了复用
                        // 为了提高发送效率允许短时间内创建多个客户端对象
                        rpcClient = sendMessage(endpoint, msg);
                        if (rpcClient != null) {
                            // 这块可以进行优化并发的发送
                            clientHolder.setSocket(rpcClient);
                        }
                    } else {
                        // 复用之间的连接如果失败后则清除连接
                        // 发送时的一切异常发生均不保留长连接
                        if (!sendMessage(endpoint, rpcClient, msg)) {
                            clientHolder.setSocket(null);
                        }
                    }
                } finally {
                    clientHolder.release();
                }
            } catch (Exception e) {
                logger.info("io executor occur exception", e);
            }
        });
    }

    /**
     * 调用socket发送对应的rpc数据
     * @param message
     * @return
     */
    public Socket sendMessage(Endpoint endpoint, byte[] message) {
        Socket rpcClient = null;
        try {
            rpcClient = new Socket();
            rpcClient.connect(new InetSocketAddress(endpoint.getHost(), endpoint.getPort()), RaftNode.connectTimeout);
            if (!sendMessage(endpoint, rpcClient, message)) {
                rpcClient = null; // 发送失败的时候sendMessage会被自动关闭这里置空即可
            }
        } catch (Throwable e){
            if (e instanceof SocketTimeoutException) {
                logger.info("try connect timeout {}", endpoint);
            } else {
                logger.info("send message failed {}", endpoint);
            }
            if (rpcClient != null) { // 连接对端时发生了异常则关闭连接
                try {
                    rpcClient.close(); // 可能被提前关闭了
                } catch (Exception se) {
                    // Ignore exception
                } finally {
                    rpcClient = null; // 必须设置为null否则socket会被复用作为长连接
                }
            }
        } finally {
            return rpcClient;
        }
    }

    /**
     * 调用socket发送对应的rpc数据
     * @param rpcClient
     * @param message
     * @return
     */
    private boolean sendMessage(Endpoint endpoint, Socket rpcClient, byte[] message) {
        boolean sendResult = true;
        try {
            OutputStream stream = rpcClient.getOutputStream();
            stream.write(message);
            stream.flush();
        } catch (SocketTimeoutException e) {
            sendResult = false;
            logger.info("send msg timeout ", endpoint);
        } catch (Exception e) {
            sendResult = false;
            logger.info("send msg failed {}", endpoint, e);
        } finally {
            if (!sendResult) { // 发送消息失败时关闭socket对象
                try {
                    rpcClient.close();
                } catch (Exception se) {
                    // Ignore exception
                }
            }
            return sendResult;
        }
    }

}
