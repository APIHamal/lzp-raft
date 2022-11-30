package com.lizhengpeng.lraft.core;

import cn.hutool.core.collection.CollUtil;
import com.lizhengpeng.lraft.exception.RaftException;
import com.lizhengpeng.lraft.request.ClientRequestMsg;
import com.lizhengpeng.lraft.request.RefreshLeaderMsg;
import com.lizhengpeng.lraft.response.AppendResult;
import com.lizhengpeng.lraft.response.RedirectRes;
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

    private static final int MAX_REDIRECT_COUNT = 5; // 自动重定向的最大次数

    private static final int CONNECT_TIME_OUT = 3000; // 连接超时时间

    private static final int READ_TIME_OUT = 5000; // 读超时时间

    private List<Endpoint> endpoints = new ArrayList<>();

    private ConcurrentHashMap<Endpoint, RpcClientHolder> rpcClientHolder = new ConcurrentHashMap<>();

    private volatile Endpoint leaderEndpoint;

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
     * 发送请求到raft集群
     * @param requestMsg
     * @return
     */
    public synchronized AppendResult sendRequestSync(ClientRequestMsg requestMsg) {
        if (leaderEndpoint == null) { // raft集群中的leader服务器的地址未知
            refreshRaftLeader(); // 重新获取一次leader的地址
            if (leaderEndpoint == null) {
                // 如果仍然没有获取到数据说明当前raft集群没有leader
                // 避免时间浪费直接返回错误
                throw new RaftException("leader endpoint un know");
            }
        }
        return autoRedirectSendRequest(leaderEndpoint, requestMsg, 0);
    }

    /**
     * 发送请求时如果遇到leadership则自动重定向到正确的服务器
     * @param endpoint
     * @param clientRequestMsg
     * @param redirectCount
     * @return
     */
    private synchronized AppendResult autoRedirectSendRequest(Endpoint endpoint, ClientRequestMsg clientRequestMsg, int redirectCount) {
        if (redirectCount > MAX_REDIRECT_COUNT) { // 重定向达到阈值则直接报错处理
            leaderEndpoint = null; // 多次重定向发生了错误则清楚leader的地址
            throw new RaftException("send msg failed");
        }
        Object response = sendMessageSync(endpoint, clientRequestMsg);
        if (redirectCount == 0 && response == null) { // 首次发送失败可能是raft发生了leadership
            refreshRaftLeader();      // 或者leader发生了宕机
            if (leaderEndpoint == null) {
                throw new RaftException("leader endpoint un know");
            }
            return autoRedirectSendRequest(leaderEndpoint, clientRequestMsg, ++redirectCount);
        } else if (response instanceof RedirectRes) {
            RedirectRes res = (RedirectRes) response;
            logger.info("receive redirect response => {}", res);
            if (res.getRedirect() == Boolean.TRUE) {
                leaderEndpoint = res.getEndpoint();
                return autoRedirectSendRequest(leaderEndpoint, clientRequestMsg, ++redirectCount);
            } else {
                leaderEndpoint = null;
                throw new RaftException("leader endpoint un know");
            }
        } else if (response instanceof AppendResult) {
            return (AppendResult) response;
        } else {
            leaderEndpoint = null;
            throw new RaftException("send msg failed");
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

}
