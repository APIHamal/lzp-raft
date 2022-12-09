package com.lizhengpeng.lraft.core;

import cn.hutool.core.collection.CollUtil;
import com.lizhengpeng.lraft.request.RefreshLeaderMsg;
import com.lizhengpeng.lraft.response.RefreshLeaderRes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * raft集群客户端的实现
 * @author lzp
 */
public class RaftClient {

    private static final Logger logger = LoggerFactory.getLogger(RaftClient.class);

    private int MAX_REDIRECT_COUNT = 5; // 自动重定向的最大次数

    private int CONNECT_TIME_OUT = 300; // 连接超时时间

    private int READ_TIME_OUT = 1000 * 10; // 读超时时间

    private List<Endpoint> endpoints = new ArrayList<>();

    private ConcurrentHashMap<Endpoint, RpcClientHolder> rpcClientHolder = new ConcurrentHashMap<>();

    private volatile Endpoint leaderEndpoint;

    /**
     * 设置客户端的连接超时时间
     * @param connectTimeout
     */
    public void setConnectTimeout(int connectTimeout) {
        this.CONNECT_TIME_OUT = connectTimeout;
    }

    /**
     * 设置读取响应的超时时间
     * @param readTimeout
     */
    public void setReadTimeout(int readTimeout) {
        this.READ_TIME_OUT = readTimeout;
    }

    /**
     * 初始化集群的节点配置
     * @param endpoints
     */
    public void addEndpoints(List<Endpoint> endpoints) {
        if (CollUtil.isNotEmpty(endpoints)) {
            this.endpoints.addAll(endpoints);
        }
    }

    /**
     * 提交任务到raft集群中执行
     * @param task
     * @return
     */
    public synchronized Status submitSync(Task task) {
        Status status = Status.failed("submit task failed");
        try {
            if (leaderEndpoint == null) { // raft集群中的leader服务器的地址未知
                refreshRaftLeader(); // 重新获取一次leader的地址
                if (leaderEndpoint == null) {
                    // 如果仍然没有获取到数据说明当前raft集群没有leader
                    // 避免时间浪费直接返回错误
                    status.setReason("there is currently no leader, please try again later");
                    return status;
                }
            }
            return autoRedirectSendRequest(leaderEndpoint, task, 0);
        } catch (Exception e) {
            logger.debug("send msg failed", e);
        }
        return status;
    }

    /**
     * 发送请求时如果遇到leadership则自动重定向到正确的服务器
     * @param endpoint
     * @param task
     * @param redirectCount
     * @return
     */
    private synchronized Status autoRedirectSendRequest(Endpoint endpoint, Task task, int redirectCount) {
        if (redirectCount > MAX_REDIRECT_COUNT) { // 重定向达到阈值则直接报错处理
            leaderEndpoint = null; // 多次重定向发生了错误则清楚leader的地址
            return Status.failed("send msg failed, maximum number of retries redirect");
        }
        Object response = sendMessageSync(endpoint, task);
        if (response != null && (response instanceof Status)) { // 首次发送失败可能是raft发生了leadership
            Status status = (Status) response;
            if (status.onRedirect()) { // 表示发生了leadership需要重新获取leader的地址
                refreshRaftLeader();
                if (leaderEndpoint == null) { // 或者leader发生了宕机
                    return Status.failed("there is currently no leader, please try again later");
                }
                return autoRedirectSendRequest(leaderEndpoint, task, ++redirectCount);
            } else { // 写入成功或者失败直接返回
                return status;
            }
        } else {
            leaderEndpoint = null;
            return Status.failed("send msg failed, please try again later");
        }
    }

    /**
     * 刷新raft集群的leader节点
     * 该方法会强制刷新
     * @return
     */
    private synchronized Endpoint refreshRaftLeader() {
        RefreshLeaderMsg refreshMsg = new RefreshLeaderMsg();
        for (Endpoint endpoint : endpoints) {
            RefreshLeaderRes res = (RefreshLeaderRes) sendMessageSync(endpoint, refreshMsg);
            logger.debug("refresh leader response => {}", res);
            if (res != null && res.getRefreshed() == Boolean.TRUE) {
                leaderEndpoint = res.getEndpoint();
                logger.debug("fetch leader endpoint => {}", leaderEndpoint);
                return leaderEndpoint;
            }
        }
        // 当前raft集群不存在leader节点时置为空
        leaderEndpoint = null;
        return leaderEndpoint;
    }

    /**
     * 发送数据到指定的节点
     * @param endpoint
     * @param msg
     * @return
     */
    private Object sendMessageSync(Endpoint endpoint, Object msg) {
        RpcClientHolder clientHolder = rpcClientHolder.get(endpoint);
        if (clientHolder == null) {
            rpcClientHolder.putIfAbsent(endpoint, new RpcClientHolder());
            clientHolder = rpcClientHolder.get(endpoint);
        }
        try {
            clientHolder.lock();
            Socket rpcClient = clientHolder.getSocket();
            if (rpcClient == null) {
                rpcClient = new Socket();
                rpcClient.connect(new InetSocketAddress(endpoint.getHost(), endpoint.getPort()), CONNECT_TIME_OUT);
                rpcClient.setSoTimeout(READ_TIME_OUT); // 超时时间设置为3秒
                clientHolder.setSocket(rpcClient);
            }
            if (!sendMessage(endpoint, rpcClient, RaftCodec.encode(msg))) {
                cleanRaftClient(clientHolder);
                return null;
            }
            return RaftCodec.decode(RpcServer.readRpcMessage(rpcClient.getInputStream()));
        } catch (SocketTimeoutException e) {
            logger.debug("read msg from => {} timeout ", endpoint);
            cleanRaftClient(clientHolder);
        } catch (IOException e) {
            logger.debug("send/read message to => {} failed", endpoint);
            cleanRaftClient(clientHolder);
        } catch (Exception e) {
            logger.debug("send/read message to => {} exception", endpoint, e);
            cleanRaftClient(clientHolder);
        } finally {
            clientHolder.release();
        }
        return null;
    }

    /**
     * 清楚rpcClient对象
     * @param clientHolder
     */
    private void cleanRaftClient(RpcClientHolder clientHolder) {
        if (clientHolder != null) {
            try {
                if (clientHolder.getSocket() != null) {
                    clientHolder.getSocket().close();
                }
            } catch (Exception e) {
                // Ignore exception
            } finally {
                clientHolder.setSocket(null);
            }
        }
    }

    /**
     * 调用socket发送对应的rpc数据
     * @param rpcClient
     * @param message
     * @return
     */
    private boolean sendMessage(Endpoint endpoint, Socket rpcClient, byte[] message) {
        try {
            OutputStream stream = rpcClient.getOutputStream();
            stream.write(message);
            stream.flush();
            return true;
        } catch (SocketTimeoutException e) {
            logger.debug("send msg to => {} timeout ", endpoint);
        } catch (Exception e) {
            logger.debug("send msg failed {}", endpoint, e);
        }
        return false;
    }

    /**
     * 清理资源
     * 释放连接到远程的socket对象
     */
    public void shutdown() {
        rpcClientHolder.values().forEach(holder -> {
            if (holder != null) {
                cleanRaftClient(holder);
            }
        });
    }

}
